
% MNESIA
-record(edfs_node, {
    host  :: atom(),
    node :: atom(),
    space_avail = 0 :: number(),
    space_alloc = 0 :: number(),
    used = false :: boolean()
}).

% MNESIA
-record(edfs_blob, {
    blob_id :: binary(),
    size :: number(),
    replica :: number(),
    hosts = []:: list(atom())
}).


% MNESIA
-record(edfs_tag, {
    path :: binary(),
    props = [] :: list({term(),term()}),
    blobs = [] :: list(binary()),
    children = []  :: list(binary())
}).
