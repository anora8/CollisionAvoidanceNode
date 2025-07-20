string requestor_id
int32 transaction_id #id of transaction (number of transaction)
int32 priority_value #[0...Nmax tries for transaction the movement]
bool is_request
geometry_msgs/Vector3  start
geometry_msgs/Vector3  goal
bool can_move

# can_move only for response
