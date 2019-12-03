package lunar

// Compact option used when opening the database
// Compaction only happens once when the data table is loaded
// The existing database will be copied to a backup file and a new database table created
func Compact(c bool) func(db *DB) error {
	return func(db *DB) error {
		db.compaction = c
		return nil
	}
}
