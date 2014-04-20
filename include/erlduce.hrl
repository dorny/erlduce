
% MNESIA
-record(edfs_node, {
    host  :: atom(),
    node :: atom(),
    space = 0 :: number(),
    used = false :: boolean()
}).

% MNESIA
-record(edfs_blob, {
    id :: binary(),
    size :: number(),
    replicas :: number(),
    hosts = []:: list(atom())
}).


% MNESIA
-record(edfs_tag, {
    path :: binary(),
    blobs = [] :: list(binary()),
    children = []  :: list(binary())
}).
