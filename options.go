package bigqueue

type Options struct {

	// size in bytes of a data page
	DataPageSize int

	// size in bytes of a index page
	indexPageSize int

	// if true enable write lock on gc action
	GcLock bool

	// the item count is  1 << IndexItemsPerPage
	IndexItemsPerPage int

	itemsPerPage int
}
