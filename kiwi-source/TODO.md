- [ ] Handle cases where requests < threads number
- [ ] Free allocated space for thread arguments
- [ ] Sync with the merge thread
- [ ] Implement mix function
- [ ] Perhaps by creating a higher order lock, we might be able to prevent the 
deadlock 

For Example:
acquire compaction_lock, acquire cv_lock, acquire lock and
acquire compaction_lock, acquire lock, acquire cv_lock
should theoretically never class, as only the first thread that acquires the 
compaction_lock will continue, while the rest of them will block.
