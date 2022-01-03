# Deleting

It's possible to achieve deletion by using the `Delete` function. It provides logical deletion, it means that it is not phisically deleted from db, but it's not possible to query it anymore after deletion. When immudb is used as an embedded store, it's possible to retrieve deleted entries. It's also possible to see deleted entries from the sdks using History endpoint, it will display if the entry was deleted.
