#include <pthread.h>
#include <string.h>
#include <assert.h>
#include "memtable.h"
#include "db.h"
#include "utils.h"
#include "indexer.h"

MemTable* memtable_new(Log* log)
{
    MemTable* self = malloc(sizeof(MemTable));

    if (!self)
        PANIC("NULL allocation");

    self->list = skiplist_new(SKIPLIST_SIZE);
    skiplist_acquire(self->list);

    self->needs_compaction = 0;
    self->add_count = 0;
    self->del_count = 0;

    self->log = log;
    self->lsn = 0;

    log_recovery(log, self->list);

    return self;
}

void memtable_reset(MemTable* self)
{
    if (self->list)
        skiplist_release(self->list);

    self->list = skiplist_new(SKIPLIST_SIZE);
    skiplist_acquire(self->list);

    log_next(self->log, ++self->lsn);

    self->needs_compaction = 0;
    self->add_count = 0;
    self->del_count = 0;
}

void memtable_free(MemTable* self)
{
    if (self->list)
        skiplist_release(self->list);
    free(self);
}

static int _memtable_edit(MemTable* self, const Variant* key, const Variant* value, OPT opt)
{
    // Here we need to insert the new node that has as a skipnode's key
    // an encoded string that encompasses both the key and value supplied
    // by the user.
    //
    
    
    size_t klen = varint_length(key->length);          // key length
    size_t vlen = (opt == DEL) ? 1 : varint_length(value->length + 1);    // value length - 0 is reserved for tombstone
    size_t encoded_len = klen + vlen + key->length + value->length;

    if (opt == DEL)
        assert(value->length == 0);

    char *mem = malloc(encoded_len);
    char *node_key = mem;

    encode_varint32(node_key, key->length);
    node_key += klen;

    memcpy(node_key, key->mem, key->length);
    node_key += key->length;

    if (opt == DEL)
        encode_varint32(node_key, 0);
    else
        encode_varint32(node_key, value->length + 1);

    node_key += vlen;
    memcpy(node_key, value->mem, value->length);

    // Sync with needs_compactions
    // If compaction is not occuring, writers might be able to sync in smaller area

    self->needs_compaction = log_append(self->log, mem, encoded_len);

    pthread_mutex_lock(&self->list->lock);
    while (self->list->readers_active > 0)
        pthread_cond_wait(&self->list->writter_cond, &self->list->lock);

    self->list->writers_active = 1;

    if (skiplist_insert(self->list, key->mem, key->length, opt, mem) == STATUS_OK_DEALLOC)
        free(mem);

    self->list->writers_active = 0;
    pthread_cond_broadcast(&self->list->reader_cond);
    pthread_mutex_unlock(&self->list->lock);

    if (opt == ADD)
        self->add_count++;
    else
        self->del_count++;
    
//    DEBUG("memtable_edit: %.*s %.*s opt: %d", key->length, key->mem, value->length, value->mem, opt);
    return 1;
}

int memtable_add(MemTable* self, const Variant* key, const Variant* value)
{
    return _memtable_edit(self, key, value, ADD);
}

int memtable_remove(MemTable* self, const Variant* key)
{
    Variant value;
    value.length = 0;
    return _memtable_edit(self, key, &value, DEL);
}

int memtable_get(SkipList* list, const Variant *key, Variant* value)
{
    pthread_mutex_lock(&list->lock);
    while(list->writers_active > 0)
        pthread_cond_wait(&list->reader_cond, &list->lock);
    list->readers_active++;
    pthread_mutex_unlock(&list->lock);

    SkipNode* node = skiplist_lookup(list, key->mem, key->length);

    pthread_mutex_lock(&list->lock);
    list->readers_active--;
    if (list->readers_active == 0) {
        pthread_cond_signal(&list->writter_cond);
    }
    pthread_mutex_unlock(&list->lock);

    if (!node)
        return 0;

    const char* encoded = node->data;
    encoded += varint_length(key->length) + key->length;

    uint32_t encoded_len = 0;
    encoded = get_varint32(encoded, encoded + 5, &encoded_len);

    if (encoded_len > 1)
        buffer_putnstr(value, encoded, encoded_len - 1);
    else
        return 0;

    return 1;
}

int memtable_needs_compaction(MemTable *self)
{
    return (self->needs_compaction ||
            self->list->count >= SKIPLIST_SIZE ||
            self->list->allocated >= MAX_SKIPLIST_ALLOCATION);
}

void memtable_extract_node(SkipNode* node, Variant* key, Variant* value, OPT* opt)
{
    uint32_t length = 0;
    const char* encoded = node->data;
    encoded = get_varint32(encoded, encoded + 5, &length);

    buffer_clear(key);
    buffer_putnstr(key, encoded, length);

    encoded += length;

    length = 0;
    encoded = get_varint32(encoded, encoded + 5, &length);

    if (length == 0)
        *opt = DEL;
    else
        *opt = ADD;

    if (value)
    {
        buffer_clear(value);

        if (*opt == ADD)
            buffer_putnstr(value, encoded, length - 1);

//        DEBUG("memtable_extract_node: %.*s %.*s opt: %d", key->length, key->mem, value->length, value->mem, *opt);
    }
}
