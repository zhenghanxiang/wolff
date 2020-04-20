-ifndef(WOLFF_HRL).
-define(WOLFF_HRL, true).

-define(conn_down(Reason), {down, Reason}).
-define(conn_error(Reason), {error, Reason}).
-define(leader_connection(Pid), {leader_connection, Pid}).
-define(UNKNOWN_OFFSET, -1).

-endif.