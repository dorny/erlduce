-module(edfs_lib).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    blob_id/2,
    get_available_space/1,
    parent_path/1,
    children/2
]).

-define( BLOB_ORD_LEN, 7).


blob_id(Path, Ord) ->
    OrdStr = if
        Ord >= ?BLOB_ORD_LEN ->
            integer_to_list(Ord);
        true ->
            L = integer_to_list(Ord),
            string:right(L, ?BLOB_ORD_LEN - length(L), $0)
    end,
    BlobIdParts = [ binary:replace(Path, <<"/">>, <<"-">>, [global]), <<"-">>, list_to_binary(OrdStr) ],
    erlang:iolist_to_binary(BlobIdParts).


get_available_space(Host) ->
    Node = erlduce_utils:node(edfs,Host),
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


children(Path,Children) ->
    lists:map(fun
        ({link, Link}) -> Link;
        (Child) -> filename:join(Path, Child)
    end, Children).
