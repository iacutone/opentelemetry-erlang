%%%------------------------------------------------------------------------
%% Copyright 2026, OpenTelemetry Authors
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc Base2 Exponential Bucket Histogram Aggregation.
%%
%% https://opentelemetry.io/docs/specs/otel/metrics/sdk/#base2-exponential-bucket-histogram-aggregation
%%
%% A value's magnitude maps to a bucket index with the logarithm mapping:
%%
%%   index(value, scale) = ceil(log2(value) * 2^scale) - 1
%%
%% Unlike the other aggregations, the bucket set is not a fixed-size
%% `counters' array: a value outside the tracked index range can force every
%% bucket to be merged pairwise (lowering the scale) to stay within
%% `max_size'. Each series' buckets are therefore kept as sparse
%% `#{Index => Count}' maps in the ETS row, and `aggregate/7' replaces the
%% whole row with a compare-and-swap, retrying if another process won.
%% @end
%%%-------------------------------------------------------------------------
-module(otel_aggregation_exponential_histogram).

-behaviour(otel_aggregation).

-export([init/2,
         aggregate/7,
         collect/4,
         default_options/0,

         %% exported for tests
         map_to_index/2,
         rescale_buckets/2,
         needed_scale_reduction/3,
         update/2,
         to_dense_buckets/1]).

-include("otel_metrics.hrl").
-include_lib("opentelemetry_api_experimental/include/otel_metrics.hrl").
-include("otel_view.hrl").

-type t() :: #exponential_histogram_aggregation{}.

-export_type([t/0]).

%% https://opentelemetry.io/docs/specs/otel/metrics/sdk/#base2-exponential-bucket-histogram-aggregation
-define(DEFAULT_MAX_SIZE, 160).
-define(DEFAULT_MAX_SCALE, 20).
-define(DEFAULT_ZERO_THRESHOLD, 0.0).

-define(MIN_DOUBLE, -9223372036854775807.0). %% the proto representation of size `fixed64'

-spec default_options() -> map().
default_options() ->
    #{max_size => ?DEFAULT_MAX_SIZE,
      max_scale => ?DEFAULT_MAX_SCALE,
      zero_threshold => ?DEFAULT_ZERO_THRESHOLD,
      record_min_max => true}.

init(#stream{name=Name,
             reader=ReaderId,
             aggregation_options=Options,
             forget=Forget}, Attributes) when is_reference(ReaderId) ->
    Generation = case Forget of
                     true ->
                         otel_metric_reader:checkpoint_generation(ReaderId);
                     _ ->
                         0
                 end,
    Key = {Name, Attributes, ReaderId, Generation},
    MaxSize = maps:get(max_size, Options, ?DEFAULT_MAX_SIZE),
    MaxScale = maps:get(max_scale, Options, ?DEFAULT_MAX_SCALE),
    ZeroThreshold = maps:get(zero_threshold, Options, ?DEFAULT_ZERO_THRESHOLD),
    RecordMinMax = maps:get(record_min_max, Options, true),
    #exponential_histogram_aggregation{key=Key,
                                       start_time=opentelemetry:timestamp(),
                                       max_size=MaxSize,
                                       max_scale=MaxScale,
                                       zero_threshold=ZeroThreshold,
                                       record_min_max=RecordMinMax,
                                       checkpoint=undefined,
                                       %% start at the finest resolution and only ever lower it
                                       scale=MaxScale,
                                       zero_count=0,
                                       positive_buckets=#{},
                                       negative_buckets=#{},
                                       min=infinity, %% works because any atom is > any integer
                                       max=?MIN_DOUBLE,
                                       sum=0,
                                       count=0
                                      }.

aggregate(Ctx, Table, ExemplarsTab, Stream = #stream{name=Name,
                                                     reader=ReaderId,
                                                     forget=Forget},
         Value, Attributes, DroppedAttributes) when is_reference(ReaderId) ->
    Generation = case Forget of
                     true ->
                         otel_metric_reader:checkpoint_generation(ReaderId);
                     _ ->
                         0
                 end,
    Key = {Name, Attributes, ReaderId, Generation},
    case ets:lookup(Table, Key) of
        [] ->
            %% `otel_aggregation:maybe_init_aggregate' inserts the row and retries
            false;
        [Old = #exponential_histogram_aggregation{}] ->
            New = update(Old, Value),
            %% map patterns in a match spec only match a subset of keys, so a
            %% concurrent write that added a bucket would still match `Old's
            %% bucket maps. `count' changes on every update, which is what
            %% makes this an exact compare-and-swap.
            case ets:select_replace(Table, [{Old, [], [{const, New}]}]) of
                1 ->
                    %% TODO: exemplars
                    true;
                0 ->
                    aggregate(Ctx, Table, ExemplarsTab, Stream, Value, Attributes, DroppedAttributes)
            end
    end.

-spec update(t(), number()) -> t().
update(Agg = #exponential_histogram_aggregation{zero_threshold=ZeroThreshold,
                                                zero_count=ZeroCount}, Value)
  when abs(Value) =< ZeroThreshold ->
    record_summary_stats(Agg#exponential_histogram_aggregation{zero_count=ZeroCount + 1}, Value);
update(Agg = #exponential_histogram_aggregation{scale=Scale,
                                                max_size=MaxSize,
                                                positive_buckets=Positive,
                                                negative_buckets=Negative},
      Value) ->
    Buckets = case Value > 0 of
                 true -> Positive;
                 false -> Negative
             end,
    Index = map_to_index(abs(Value), Scale),

    {NewScale, NewBuckets} =
        case map_size(Buckets) of
            0 ->
                {Scale, #{Index => 1}};
            _ ->
                CurrentIndexes = maps:keys(Buckets),
                MinIndex = min(lists:min(CurrentIndexes), Index),
                MaxIndex = max(lists:max(CurrentIndexes), Index),
                case needed_scale_reduction(MinIndex, MaxIndex, MaxSize) of
                    0 ->
                        {Scale, maps:update_with(Index, fun(C) -> C + 1 end, 1, Buckets)};
                    Reduction ->
                        Rescaled = rescale_buckets(Buckets, Reduction),
                        RescaledIndex = Index bsr Reduction,
                        {Scale - Reduction,
                         maps:update_with(RescaledIndex, fun(C) -> C + 1 end, 1, Rescaled)}
                end
        end,

    %% scale is shared by both bucket sets, so a downscale must also be
    %% applied to the side this value didn't touch
    ScaleDelta = Scale - NewScale,
    Agg1 = case Value > 0 of
               true ->
                   Agg#exponential_histogram_aggregation{
                     positive_buckets=NewBuckets,
                     negative_buckets=rescale_buckets(Negative, ScaleDelta)};
               false ->
                   Agg#exponential_histogram_aggregation{
                     negative_buckets=NewBuckets,
                     positive_buckets=rescale_buckets(Positive, ScaleDelta)}
           end,

    record_summary_stats(Agg1#exponential_histogram_aggregation{scale=NewScale}, Value).

record_summary_stats(Agg = #exponential_histogram_aggregation{record_min_max=false,
                                                              sum=Sum,
                                                              count=Count}, Value) ->
    Agg#exponential_histogram_aggregation{sum=Sum + Value, count=Count + 1};
record_summary_stats(Agg = #exponential_histogram_aggregation{record_min_max=true,
                                                              min=Min,
                                                              max=Max,
                                                              sum=Sum,
                                                              count=Count}, Value) ->
    Agg#exponential_histogram_aggregation{min=erlang:min(Min, Value),
                                         max=erlang:max(Max, Value),
                                         sum=Sum + Value,
                                         count=Count + 1}.

%% `Value' must be positive; callers pass the magnitude and handle zero
%% separately.
%%
%% `math:log2/1' is floating point, so a value within a few ULPs of a bucket
%% boundary (without being exactly on it) can land in the adjacent bucket.
%% The spec permits this logarithm method; an exact alternative for
%% `scale =< 0' would extract the IEEE-754 exponent directly.
-spec map_to_index(pos_integer() | float(), integer()) -> integer().
map_to_index(Value, Scale) when Value > 0 ->
    trunc(math:ceil(math:log2(Value) * math:pow(2, Scale))) - 1.

%% the number of times the inclusive index range must be halved to fit in
%% `MaxSize' buckets
-spec needed_scale_reduction(integer(), integer(), pos_integer()) -> non_neg_integer().
needed_scale_reduction(MinIndex, MaxIndex, MaxSize) ->
    needed_scale_reduction(MinIndex, MaxIndex, MaxSize, 0).

needed_scale_reduction(MinIndex, MaxIndex, MaxSize, N) ->
    case (MaxIndex bsr N) - (MinIndex bsr N) + 1 =< MaxSize of
        true -> N;
        false -> needed_scale_reduction(MinIndex, MaxIndex, MaxSize, N + 1)
    end.

%% merges every two adjacent buckets `Shift' times, summing their counts
-spec rescale_buckets(#{integer() => pos_integer()}, non_neg_integer()) -> #{integer() => pos_integer()}.
rescale_buckets(Buckets, 0) ->
    Buckets;
rescale_buckets(Buckets, Shift) ->
    maps:fold(fun(Index, Count, Acc) ->
                     maps:update_with(Index bsr Shift, fun(C) -> C + Count end, Count, Acc)
             end, #{}, Buckets).

checkpoint(Tab, #stream{name=Name,
                        reader=ReaderId,
                        temporality=?TEMPORALITY_DELTA}, Generation) ->
    Key = {Name, '_', ReaderId, Generation},
    MS = [{#exponential_histogram_aggregation{key=Key, _='_'}, [], ['$_']}],
    Rows = ets:select(Tab, MS),
    lists:foreach(
      fun(Old) ->
              New = Old#exponential_histogram_aggregation{checkpoint={snapshot(Old)}},
              %% if a concurrent `aggregate/7' won, the checkpoint is a slightly
              %% stale snapshot, the same tradeoff `otel_aggregation_histogram_explicit' makes
              _ = ets:select_replace(Tab, [{Old, [], [{const, New}]}])
      end, Rows),
    ok;
checkpoint(_Tab, _, _) ->
    ok.

collect(Tab, _ExemplarsTab, Stream=#stream{name=Name,
                                           reader=ReaderId,
                                           temporality=Temporality,
                                           forget=Forget}, Generation0) ->
    CollectionStartTime = opentelemetry:timestamp(),
    Generation = case Forget of
                     true ->
                         Generation0;
                     _ ->
                         0
                 end,

    checkpoint(Tab, Stream, Generation),

    Select = [{#exponential_histogram_aggregation{key={Name, '_', ReaderId, Generation}, _='_'}, [], ['$_']}],
    Rows = ets:select(Tab, Select),
    Result = #exponential_histogram{
                datapoints=[datapoint(CollectionStartTime, Row) || Row <- Rows],
                aggregation_temporality=Temporality},

    maybe_delete_old_generation(Tab, Name, ReaderId, Generation),

    Result.

%%

maybe_delete_old_generation(_Tab, _Name, _ReaderId, 0) ->
    ok;
maybe_delete_old_generation(Tab, Name, ReaderId, Generation) ->
    Select = [{#exponential_histogram_aggregation{key={Name, '_', ReaderId, '$1'}, _='_'},
               [{'<', '$1', {const, Generation}}],
               [true]}],
    ets:select_delete(Tab, Select).

snapshot(#exponential_histogram_aggregation{scale=Scale,
                                            zero_count=ZeroCount,
                                            positive_buckets=Positive,
                                            negative_buckets=Negative,
                                            min=Min,
                                            max=Max,
                                            sum=Sum,
                                            count=Count,
                                            start_time=StartTime}) ->
    #exponential_histogram_checkpoint{scale=Scale,
                                      zero_count=ZeroCount,
                                      positive_buckets=Positive,
                                      negative_buckets=Negative,
                                      min=Min,
                                      max=Max,
                                      sum=Sum,
                                      count=Count,
                                      start_time=StartTime}.

datapoint(CollectionStartTime, Agg=#exponential_histogram_aggregation{checkpoint=undefined}) ->
    datapoint(CollectionStartTime, Agg, snapshot(Agg));
datapoint(CollectionStartTime, Agg=#exponential_histogram_aggregation{checkpoint={Checkpoint}}) ->
    datapoint(CollectionStartTime, Agg, Checkpoint).

datapoint(CollectionStartTime,
          #exponential_histogram_aggregation{key={_, Attributes, _, _},
                                             zero_threshold=ZeroThreshold,
                                             record_min_max=RecordMinMax},
          #exponential_histogram_checkpoint{scale=Scale,
                                            zero_count=ZeroCount,
                                            positive_buckets=Positive,
                                            negative_buckets=Negative,
                                            min=Min,
                                            max=Max,
                                            sum=Sum,
                                            count=Count,
                                            start_time=StartTime}) ->
    {DatapointMin, DatapointMax} = case RecordMinMax of
                                       true -> {Min, Max};
                                       false -> {undefined, undefined}
                                   end,
    #exponential_histogram_datapoint{
       attributes=binary_to_term(Attributes),
       start_time=StartTime,
       time=CollectionStartTime,
       count=Count,
       sum=Sum,
       scale=Scale,
       zero_count=ZeroCount,
       positive=to_dense_buckets(Positive),
       negative=to_dense_buckets(Negative),
       %% TODO: exemplars
       exemplars=[],
       flags=0,
       min=DatapointMin,
       max=DatapointMax,
       zero_threshold=ZeroThreshold
      }.

%% OTLP buckets are dense: `{Offset, Counts}' where `Offset' is the index of
%% the first count and missing indexes are filled with 0
-spec to_dense_buckets(#{integer() => pos_integer()}) -> {integer(), [non_neg_integer()]}.
to_dense_buckets(Buckets) when map_size(Buckets) =:= 0 ->
    {0, []};
to_dense_buckets(Buckets) ->
    Indexes = maps:keys(Buckets),
    Offset = lists:min(Indexes),
    MaxIndex = lists:max(Indexes),
    Counts = [maps:get(I, Buckets, 0) || I <- lists:seq(Offset, MaxIndex)],
    {Offset, Counts}.
