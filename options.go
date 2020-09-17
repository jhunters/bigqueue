package bigqueue

// Options the options struct
type Options struct {

	// size in bytes of a data page
	DataPageSize int

	// size in bytes of a index page
	indexPageSize int

	// the item count is  1 << IndexItemsPerPage
	IndexItemsPerPage int

	itemsPerPage int

	// if value > 0 then enable auto gc features and repeat process gc by the specified interval time in seconds.
	AutoGCBySeconds int
}
