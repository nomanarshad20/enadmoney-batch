


consumer receives DTO
strategy/factory selects processor
processor calls service
service updates batch lifecycle
service fetches records by startId/endId using pagination
each page is submitted to executor
executor runs page tasks in parallel
wait for all futures
update batch entity final status



