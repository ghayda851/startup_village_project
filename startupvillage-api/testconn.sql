SELECT
  current_database() AS db,
  current_user AS user,
  inet_server_addr() AS server_ip,
  inet_server_port() AS server_port;