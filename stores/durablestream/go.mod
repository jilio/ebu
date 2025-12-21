module github.com/jilio/ebu/stores/durablestream

go 1.25.1

replace github.com/jilio/ebu => ../../

require (
	github.com/ahimsalabs/durable-streams-go v0.0.0-20251220072926-9430608b4163
	github.com/jilio/ebu v0.0.0
)

require github.com/go4org/hashtriemap v0.0.0-20251130024219-545ba229f689 // indirect
