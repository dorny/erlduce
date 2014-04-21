-module(edfs_lib).

-author("Michal Dorner <dorner.michal@gmail.com>").

-export([
    blob_filename/1,
    get_available_space/1,
    parent_path/1,
    children/2
]).


blob_filename({Path,Part}) ->
    {ok, WorkDir} = application:get_env(erlduce, work_dir),
    Filename = http_uri:encode( filename:join( binary_to_list(Path), integer_to_list(Part))),
    filename:join([WorkDir, <<"blobs">>, Filename]).


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
