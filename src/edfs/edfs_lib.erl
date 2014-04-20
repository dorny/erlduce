-module(edfs_lib).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    generate_id/0,
    get_available_space/1,
    parent_path/1
]).



generate_id() ->
    H1 = integer_to_binary(erlang:phash2(now())),
    H2 = integer_to_binary(erlang:phash2(make_ref())),
    H3 = integer_to_binary(erlang:phash2(make_ref())),
    S = <<"-">>,
    HashList = [H1, S, H2, S, H3],
    iolist_to_binary(HashList).


get_available_space(Node) ->
    case rpc:call(Node, os, cmd, ["df --output=avail --block-size=1 . | tail -n 1 | tr -d ' \n'"]) of
        {badrpc, Error} ->
            {error, Error};
        Space ->
            try
                erlang:list_to_integer(Space)
            catch
                Error -> {error,Error}
            end
    end.


parent_path(Path) ->
    PathParts = filename:split(Path),
    case length(PathParts) of
        1 ->
            <<"/">>;
        _ ->
            ParentParts = lists:droplast(PathParts),
            filename:join(ParentParts)
    end.
