-module(riak_mapred_basic).
-export([count_objects/0, count_words/2, connect/0, clean_bucket/2, print_bucket/2,
          count_map/3, add_int_red/2, words_dict_map/3, merge_dicts_red/2,
          add_some_data/2]).
 
% Map and reduce functions are executed on Riak nodes. This module must be loaded
% on all your riak nodes when you execute the map-reduce query.
 
% Return 1 for every object.
count_map(_G, _KeyData, none) -> [1].
 
% This function expects the data to be a binary holding text,
% and returns a dictionary with all words from that text.
words_dict_map(G, _KeyData, none) ->
   [dict:from_list([{I, 1}
    || I <- begin
              T = riak_object:get_value(G),
              string:tokens(binary_to_list(T), " ,.:;\"\'!?")
            end])].
 
% Reduce function adding integers.
add_int_red(GCounts, none) ->
  [lists:sum(GCounts)].
 
% This reduce function takes a list
% of integer-valued dictionaries, and merges them all together.
merge_dicts_red(Gcounts, none) ->
  [lists:foldl(fun(G, Acc) ->
        dict:merge(fun(_, X, Y) -> X+Y end,
        G, Acc)
    end,
    dict:new(),
    Gcounts)].
 
connect() ->
  {ok, Client} = riakc_pb_socket:start("127.0.0.1", 8087),
  Client.
 
clean_bucket_loop(P, B, Rid) ->
  receive
    {Rid, {keys, Keys}} ->
      lists:map (fun (K) -> riakc_pb_socket:delete(P, B, K) end, Keys),
      clean_bucket_loop(P, B, Rid);
    {Rid, done} -> ok
  end.
 
% Remove all objects from the bucket
clean_bucket(P, B) ->
  {ok, Rid} = riakc_pb_socket:stream_list_keys(P,B),
  clean_bucket_loop(P, B, Rid).
 
print_metadata(P, B, K) ->
  case riakc_pb_socket:get(P, B, K) of
    {ok, O} ->
      M = riakc_obj:get_metadata(O),
      io:format("~w: ", [K]),
      lists:map (fun ({N, V}) -> io:format("~s -> ~w, ", [N, V]) end, dict:to_list (M)),
      io:format("~n");
    {error, _} -> io:format("~s: not found~n", [K])
  end.
 
print_bucket_loop(P, B, Rid, N) ->
  receive
    {Rid, {keys, Keys}} ->
      lists:map (fun (K) -> print_metadata(P, B, K) end, Keys),
      print_bucket_loop(P, B, Rid, N+length(Keys));
    {Rid, done} -> N
  end.
 
% print metadata too
print_bucket(P, B) ->
  {ok, Rid} = riakc_pb_socket:stream_list_keys(P,B),
  print_bucket_loop(P, B, Rid, 0).
 
% This mapreduce job counts the number of objects in a bucket,
% but does not check for objects being tombstones, which means
% that it can count objects that have been deleted.
count_objects() ->
  X = riakc_pb_socket:mapred(
             connect(),
             <<20141215>>,
             [{map, {modfun, riak_mapred_basic, count_map}, none, false},
              {reduce, {modfun, riak_mapred_basic, add_int_red}, none, true}]),
  X.
 
% Count words interpreting values as binaries containging text.
% This function has a bug - can you find it?
count_words(P, B) ->
  X = riakc_pb_socket:mapred(
             P,
             B,
             [{map, {modfun, riak_mapred_basic, words_dict_map}, none, false},
              {reduce, {modfun, riak_mapred_basic, merge_dicts_red}, none, true}]),
  case X of
    {ok, [{1, [R]}]} -> io:format("~p~n",[
                          lists:sort(fun ({_, N1}, {_, N2}) -> N1 < N2 end, dict:to_list(R))]);
    {error, E} -> io:format("Error: ~s~n",[E])
  end,
  X.
 
% Add some text in binaries to the bucket (with arbitrary ids)
add_some_data(P, B) ->
  [add_kv(P, B, K, V) || {K, V} <- [{1, <<"word1 word2 word1">>},
                              {2, <<"word3 word4 word5">>},
                              {3, <<"word2 word4 word3">>},
                              {4, <<"word6 word7 word1">>}]].
 
add_kv(P, B, K, V) ->
  Obj = riakc_obj:new(B, list_to_binary(integer_to_list(K)), V),
  riakc_pb_socket:put(P, Obj, [{w, 1}]).