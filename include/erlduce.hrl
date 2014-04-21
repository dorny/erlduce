
% MNESIA
-record(edfs_node, {
    host  :: atom(),
    space = 0 :: number(),
    used = false :: boolean()
}).

% MNESIA
-record(edfs_blob, {
    id :: binary(),
    size :: number(),
    spec :: data_spec(),
    replicas :: number(),
    hosts = []:: list(atom())
}).


% MNESIA
-record(edfs_tag, {
    path :: binary(),
    blobs = 0 :: number(),
    children = []  :: list( binary()|{link, binary()} )
}).




-type compression() :: zip | snappy | none.
-type data_type() :: binary | term.
-type data_spec() :: data_type() | {compression(), data_type()}.
