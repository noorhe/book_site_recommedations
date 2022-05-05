rm -rf *.json *.log
echo "{\"op\":\"push\",\"type\":\"user_page_pair\",\"data\":{\"__type__\":\"UserIdPagePair\",\"userId\":\"noorhe\",\"page\":1}}" > log.json
echo "{\"user_id_page_pair\":300,\"books\":300,\"readers\":300,\"tags\":300,\"selections\":300,\"distributions\":300}" > run_settings.json
mkdir logs
