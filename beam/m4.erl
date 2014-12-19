%% -*- coding: utf-8 -*-
-module(m4).
-export([merge_dicts_red/2, words_dict_map/3, count_words/0, addUpObjects/2, countObjects/3, textToDict/1, getMostPopularWords/0, getWordDict/3, getNumberOfObjects/0, sortReduce/2,connect/0]).

count_words() ->
  X = riakc_pb_socket:mapred_bucket(
             connect(),
             <<20141215>>,
             [{map, {modfun, m4, words_dict_map}, none, false},
              {reduce, {modfun, m4, merge_dicts_red}, none, true}]),
  %{ok, [{1, [R]}]} = X,
  %S = lists:sort(fun ({_, N1}, {_, N2}) -> N1 < N2 end, dict:to_list(R)),
  
  case X of
    {ok, [{1, [R]}]} -> io:format("~p~n",[
                          lists:sort(fun ({_, N1}, {_, N2}) -> N1 < N2 end, dict:to_list(R))]);
    {error, E} -> io:format("Error: ~s~n",[E])
  end,
  X.

words_dict_map(G, _KeyData, none) ->
   [dict:from_list([{I, 1}
    || I <- begin
              T = riak_object:get_value(G),
              string:tokens(binary_to_list(T), " ,.:;\"\'!?")
            end])].

merge_dicts_red(Gcounts, none) ->
  [lists:foldl(fun(G, Acc) ->
        dict:merge(fun(_, X, Y) -> X+Y end,
        G, Acc)
    end,
    dict:new(),
    Gcounts)].


getNumberOfObjects() ->
  {ok, [{_N, Result}]}  = riakc_pb_socket:mapred_bucket(
    connect(),
    <<"20141215">>,
    [{map, {modfun, ?MODULE, countObjects}, none, false},
    {reduce, {modfun, ?MODULE, addUpObjects}, none, true}]),
  %io:format("Number of RiakObjects: ~p~n", [Result]).
  Result.

countObjects(_RiakObject, _KeyData, _Arg) -> [1].
addUpObjects(GCounts, none) -> [lists:sum(GCounts)].

getMostPopularWords() ->
  {ok, [{_N, Result}]}  = riakc_pb_socket:mapred_bucket(
    connect(),
    <<"20141215">>,
    [{map, {modfun, ?MODULE, getWordDict}, none, false},
    {reduce, {modfun, ?MODULE, merge_dicts_red}, none, true}]),
  hd(hd(Result)).

getWordDict(RiakObject, _KeyData, _Arg) ->
  TweetInBinary = riak_object:get_value(RiakObject),
  TweetInString = unicode:characters_to_list(TweetInBinary),
  TweetText = getText(TweetInString),
  TweetTextDict = textToDict(TweetText),
  [TweetTextDict].

getText(TweetData) ->
  % Raw text string that marks where the text start
  %TextString =  "\\\"text\\\":\\\"",
  TextString = "text",
  % Raw text string that marks where the text ends
  %SourceString = "\",\\\"source\\\"",
  SourceString = "source",
  % Get where the TextString starts and adds it length to position, to get the end
  TextStart = string:str(TweetData, TextString)+string:len(TextString)+3,
  % Length = End - Start
  TextLength = string:str(TweetData, SourceString) - TextStart-3,
  Text = string:substr(TweetData, TextStart, TextLength),
  Text.

getLang(TweetDate) ->
  LangStart = string:str(TweetData, "lang")+string:len("lang")+3,
  LangLength = 2,
  Lang = string:substr(TweetData, LangStart, LangLength),
  Lang.



textToDict(Text) ->
  WordList = string:tokens(Text, " "),
  dict:from_list([{I, 1} || I <- WordList]).


sortReduce(ValueList, _Arg) ->
  lists:sort(ValueList).

connect() ->
  {ok, Client} = riakc_pb_socket:start("127.0.0.1", 8087),
  Client.



%% Function to find subString location in a string. Used this before we found
%% that there is a BIF for that

% find(String, Target) -> find(String, Target, -1) - length(Target) + 1.
% find([], _, Acc) -> Acc;
% find(_, [], Acc) -> Acc;
% find([Same|StringTail], [Same|TargetTail], Acc) ->
%   find(StringTail, TargetTail, Acc + 1);
% find([_|StringTail], Target, Acc) ->
%   find(StringTail, Target, Acc + 1).
