
-type compression() :: zip | snappy | none.
-type data_type() :: binary | term.
-type data_spec() :: data_type() | {compression(), data_type()}.

-type inode() :: number().
-type blob_count() :: number().
-type file_type() :: directory | regular.
-type file_ref() :: {binary(), inode()}.
-type file_list() :: list( file_ref()).

% MNESIA
-record(edfs_prop, {
    key :: term(),
    value :: term()
}).


% MNESIA
-record(edfs_blob, {
    id :: {inode(), number()},
    size :: number(),
    replicas :: number(),
    hosts = [] :: list(atom())
}).


% MNESIA
-record(edfs_rec, {
    inode :: inode(),
    type :: file_type(),
    children :: blob_count() | gb_trees:gb_tree(file_ref())
}).

% MNESIA
-record(edfs_inodes, {
    inode :: number(),
    useless = true
}).
