#if defined(_WIN32)
#include <direct.h>
#else
#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <limits.h>
#include <errno.h>
#include <stdatomic.h>
#if defined(_WIN32)
#include <windows.h>
#include <process.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#endif
#if defined(_WIN32) && defined(__MINGW32__) && !defined(BASALT_USE_NATIVE_C11_THREADS)
#include <pthread.h>
#include <sched.h>
typedef pthread_t thrd_t;
typedef int (*basalt_thrd_start_t)(void *);
typedef struct basalt_thrd_start_context {
  basalt_thrd_start_t entry;
  void *argument;
  int result;
} basalt_thrd_start_context;
static void *basalt_thrd_start(void *raw) {
  basalt_thrd_start_context *context = (basalt_thrd_start_context *)raw;
  context->result = context->entry(context->argument);
  return raw;
}
static int basalt_thrd_create(thrd_t *thread, basalt_thrd_start_t entry, void *argument) {
  basalt_thrd_start_context *context;
  int status;
  context = (basalt_thrd_start_context *)malloc(sizeof(*context));
  if (!context)
    return 1;
  context->entry = entry;
  context->argument = argument;
  context->result = 0;
  status = pthread_create(thread, NULL, basalt_thrd_start, context);
  if (status != 0)
    free(context);
  return status;
}
static int basalt_thrd_join(thrd_t thread, int *result) {
  void *raw = NULL;
  int status = pthread_join(thread, &raw);
  if (status == 0) {
    basalt_thrd_start_context *context = (basalt_thrd_start_context *)raw;
    if (result)
      *result = context->result;
    free(context);
  }
  return status;
}
#define thrd_success 0
#define thrd_create(thread, entry, argument) basalt_thrd_create((thread), (entry), (argument))
#define thrd_join(thread, result) basalt_thrd_join((thread), (result))
#define thrd_yield() sched_yield()
#else
#include <threads.h>
#endif
#if defined(_WIN32) && defined(__MINGW32__)
#ifndef BASALT_ALIGNED_ALLOC_DECLARED
#define BASALT_ALIGNED_ALLOC_DECLARED 1
void *aligned_alloc(size_t alignment, size_t size);
#endif
#endif
#if defined(__GNUC__) || defined(__clang__)
#define BASALT_UNUSED __attribute__((unused))
#else
#define BASALT_UNUSED
#endif
static void basalt_panic(int code) {
  (void)code;
  exit(2);
}
static size_t basalt_checked_bytes(int count, size_t elem_size) {
  if (count < 0)
    basalt_panic(1);
  if (elem_size != 0 && (size_t)count > (size_t)-1 / elem_size)
    basalt_panic(1);
  return (size_t)count * elem_size;
}
static void *basalt_track(void *);
static void basalt_release(void *);
typedef struct basalt_atomic_int {
  atomic_int value;
} basalt_atomic_int;
static BASALT_UNUSED void *basalt_atomic_make(int initial) {
  basalt_atomic_int *a = (basalt_atomic_int *)calloc(1, sizeof(*a));
  if (!a)
    basalt_panic(5);
  atomic_init(&a->value, initial);
  return basalt_track(a);
}
static BASALT_UNUSED int basalt_atomic_load(void *p) {
  basalt_atomic_int *a = (basalt_atomic_int *)p;
  if (!a)
    basalt_panic(4);
  return atomic_load_explicit(&a->value, memory_order_acquire);
}
static BASALT_UNUSED void basalt_atomic_store(void *p, int value) {
  basalt_atomic_int *a = (basalt_atomic_int *)p;
  if (!a)
    basalt_panic(4);
  atomic_store_explicit(&a->value, value, memory_order_release);
}
static BASALT_UNUSED int basalt_atomic_fetch_add(void *p, int delta) {
  basalt_atomic_int *a = (basalt_atomic_int *)p;
  if (!a)
    basalt_panic(4);
  return atomic_fetch_add_explicit(&a->value, delta, memory_order_acq_rel);
}
static BASALT_UNUSED int basalt_atomic_compare_exchange(void *p, int expected, int desired) {
  basalt_atomic_int *a = (basalt_atomic_int *)p;
  int old;
  if (!a)
    basalt_panic(4);
  old = expected;
  return atomic_compare_exchange_strong_explicit(&a->value, &old, desired, memory_order_acq_rel,
                                                 memory_order_acquire);
}
static BASALT_UNUSED void basalt_atomic_free(void *p) {
  basalt_release(p);
}
typedef struct basalt_channel {
  _Atomic size_t head;
  _Atomic size_t tail;
  atomic_int closed;
  size_t capacity;
  int data[];
} basalt_channel;
static BASALT_UNUSED void *basalt_channel_make(int requested) {
  size_t cap = 2;
  size_t bytes;
  basalt_channel *c;
  if (requested < 1 || requested > 1073741824)
    basalt_panic(7);
  while (cap < (size_t)requested) {
    if (cap > (size_t)-1 / 2)
      basalt_panic(1);
    cap *= 2;
  }
  if (cap > (size_t)-1 / sizeof(int))
    basalt_panic(1);
  bytes = sizeof(*c) + cap * sizeof(int);
  if (bytes < sizeof(*c))
    basalt_panic(1);
  c = (basalt_channel *)calloc(1, bytes);
  if (!c)
    basalt_panic(5);
  c->capacity = cap;
  atomic_init(&c->head, 0);
  atomic_init(&c->tail, 0);
  atomic_init(&c->closed, 0);
  return basalt_track(c);
}
static BASALT_UNUSED int basalt_channel_send(void *p, int value) {
  basalt_channel *c = (basalt_channel *)p;
  size_t head, tail;
  if (!c)
    basalt_panic(4);
  if (atomic_load_explicit(&c->closed, memory_order_acquire) != 0)
    return -1;
  head = atomic_load_explicit(&c->head, memory_order_relaxed);
  tail = atomic_load_explicit(&c->tail, memory_order_acquire);
  if (head - tail >= c->capacity)
    return 0;
  c->data[head & (c->capacity - 1)] = value;
  atomic_store_explicit(&c->head, head + 1, memory_order_release);
  return 1;
}
static BASALT_UNUSED int basalt_channel_recv(void *p, int *out) {
  basalt_channel *c = (basalt_channel *)p;
  size_t head, tail;
  if (!c || !out)
    basalt_panic(4);
  tail = atomic_load_explicit(&c->tail, memory_order_relaxed);
  head = atomic_load_explicit(&c->head, memory_order_acquire);
  if (tail == head) {
    if (atomic_load_explicit(&c->closed, memory_order_acquire) != 0)
      return -1;
    return 0;
  }
  *out = c->data[tail & (c->capacity - 1)];
  atomic_store_explicit(&c->tail, tail + 1, memory_order_release);
  return 1;
}
static BASALT_UNUSED void basalt_channel_close(void *p) {
  basalt_channel *c = (basalt_channel *)p;
  if (!c)
    basalt_panic(4);
  atomic_store_explicit(&c->closed, 1, memory_order_release);
}
static BASALT_UNUSED void basalt_channel_free(void *p) {
  basalt_release(p);
}
typedef struct basalt_thread_handle {
  thrd_t thread;
} basalt_thread_handle;
static BASALT_UNUSED void *basalt_thread_spawn(int (*entry)(void *), void *arg) {
  basalt_thread_handle *h = (basalt_thread_handle *)calloc(1, sizeof(*h));
  if (!h)
    basalt_panic(5);
  if (thrd_create(&h->thread, entry, arg) != thrd_success) {
    free(h);
    return NULL;
  }
  return basalt_track(h);
}
static BASALT_UNUSED int basalt_thread_join(void *p) {
  basalt_thread_handle *h = (basalt_thread_handle *)p;
  int result;
  if (!h)
    basalt_panic(4);
  if (thrd_join(h->thread, &result) != thrd_success)
    basalt_panic(8);
  basalt_release(h);
  return result;
}
static BASALT_UNUSED void basalt_thread_yield(void) {
  thrd_yield();
}
static char **basalt_inc_active = NULL;
static size_t basalt_inc_active_n = 0, basalt_inc_active_cap = 0;
static char **basalt_inc_loaded = NULL;
static size_t basalt_inc_loaded_n = 0, basalt_inc_loaded_cap = 0;
static char *basalt_inc_last_path = NULL;
static char *basalt_inc_project_root = NULL;
static int basalt_inc_status = 0;
static BASALT_UNUSED int basalt_inc_eq(const char *a, const char *b) {
  return strcmp(a, b) == 0;
}
static BASALT_UNUSED size_t basalt_inc_find(char **v, size_t n, const char *p) {
  size_t i;
  for (i = 0; i < n; i++)
    if (basalt_inc_eq(v[i], p))
      return i;
  return (size_t)-1;
}
static BASALT_UNUSED void basalt_inc_add(char ***vp, size_t *np, size_t *cp, char *p) {
  size_t c;
  char **q;
  if (*np == *cp) {
    c = *cp ? *cp * 2 : 16;
    q = (char **)realloc(*vp, c * sizeof(char *));
    if (!q)
      exit(2);
    *vp = q;
    *cp = c;
  }
  (*vp)[(*np)++] = p;
}
static BASALT_UNUSED char *basalt_inc_strdup(const char *p) {
  size_t n = strlen(p);
  char *q = (char *)malloc(n + 1);
  if (!q)
    exit(2);
  memcpy(q, p, n + 1);
  return (char *)basalt_track(q);
}
static BASALT_UNUSED char *basalt_inc_cwd(void) {
  char buf[4096];
#if defined(_WIN32)
  if (_getcwd(buf, (int)sizeof(buf)))
    return basalt_inc_strdup(buf);
#else
  if (getcwd(buf, sizeof(buf)))
    return basalt_inc_strdup(buf);
#endif
  return basalt_inc_strdup("");
}
static BASALT_UNUSED char *basalt_inc_realpath(const char *p) {
  if (p && p[0] == 0) {
    if (basalt_inc_last_path)
      return basalt_inc_last_path;
    if (basalt_inc_active_n)
      return basalt_inc_active[basalt_inc_active_n - 1];
    return basalt_inc_strdup("");
  }
#if defined(_WIN32)
  char *q = _fullpath(NULL, p, 0);
  if (q)
    return (char *)basalt_track(q);
#else
  char *q = realpath(p, NULL);
  if (q)
    return (char *)basalt_track(q);
#endif
  return basalt_inc_strdup(p);
}
static BASALT_UNUSED int basalt_inc_begin(char *p) {
  if (basalt_inc_find(basalt_inc_active, basalt_inc_active_n, p) != (size_t)-1) {
    basalt_inc_status = 1;
    return 0;
  }
  if (basalt_inc_find(basalt_inc_loaded, basalt_inc_loaded_n, p) != (size_t)-1) {
    basalt_inc_status = 2;
    return 0;
  }
  basalt_inc_add(&basalt_inc_active, &basalt_inc_active_n, &basalt_inc_active_cap, p);
  basalt_inc_status = 0;
  return 1;
}
static BASALT_UNUSED void basalt_include_close(void) {
  if (basalt_inc_active_n) {
    char *p = basalt_inc_active[--basalt_inc_active_n];
    if (basalt_inc_find(basalt_inc_loaded, basalt_inc_loaded_n, p) == (size_t)-1)
      basalt_inc_add(&basalt_inc_loaded, &basalt_inc_loaded_n, &basalt_inc_loaded_cap, p);
  }
}
static BASALT_UNUSED char *basalt_inc_join(const char *base, const char *raw) {
  const char *s = strrchr(base, '/');
  size_t n = s ? (size_t)(s - base + 1) : 0;
  size_t m = strlen(raw);
  char *q = (char *)malloc(n + m + 1);
  if (!q)
    exit(2);
  if (n)
    memcpy(q, base, n);
  memcpy(q + n, raw, m + 1);
  return (char *)basalt_track(q);
}
static BASALT_UNUSED char *basalt_inc_dir_join(const char *base, const char *raw) {
  size_t n = strlen(base);
  size_t m = strlen(raw);
  char *q;
  if (n > (size_t)-1 - m - 2)
    exit(2);
  q = (char *)malloc(n + 1 + m + 1);
  if (!q)
    exit(2);
  memcpy(q, base, n);
  if (n && base[n - 1] != '/' && base[n - 1] != '\\')
    q[n++] = '/';
  memcpy(q + n, raw, m + 1);
  return (char *)basalt_track(q);
}
static BASALT_UNUSED char *basalt_inc_expand_prefix(const char *raw) {
  const char *base = NULL;
  size_t skip = 0, n, m;
  char *q;
  if (strncmp(raw, "@stdlib/", 8) == 0) {
    base = "src/stdlib/";
    skip = 8;
  } else if (strncmp(raw, "@lib/", 5) == 0) {
    base = ".basalt/vendor/";
    skip = 5;
  } else
    return NULL;
  n = strlen(base);
  m = strlen(raw + skip);
  if (n > (size_t)-1 - m - 1)
    exit(2);
  q = (char *)malloc(n + m + 1);
  if (!q)
    exit(2);
  memcpy(q, base, n);
  memcpy(q + n, raw + skip, m + 1);
  return (char *)basalt_track(q);
}
static BASALT_UNUSED int basalt_inc_prefix_safe(const char *raw) {
  const char *p = raw;
  const char *segment;
  size_t len;
  size_t skip = 0;
  if (strncmp(raw, "@stdlib/", 8) == 0)
    skip = 8;
  else if (strncmp(raw, "@lib/", 5) == 0)
    skip = 5;
  else
    return 1;
  segment = raw + skip;
  for (p = segment;; p++) {
    if (*p == 47 || *p == 92 || *p == 0) {
      len = (size_t)(p - segment);
      if (len == 2 && segment[0] == 46 && segment[1] == 46)
        return 0;
      if (*p == 0)
        return 1;
      segment = p + 1;
    }
  }
}
static BASALT_UNUSED int basalt_include_line_mode(int *line, int n) {
  int i = 0, j;
  while (i < n && (line[i] == ' ' || line[i] == 9))
    i++;
  if (i + 7 <= n && line[i] == 'i' && line[i + 1] == 'n' && line[i + 2] == 'c' &&
      line[i + 3] == 'l' && line[i + 4] == 'u' && line[i + 5] == 'd' && line[i + 6] == 'e') {
    j = i + 7;
    while (j < n && (line[j] == ' ' || line[j] == 9))
      j++;
    if (j < n && line[j] == 34)
      return 1;
  }
  if (i + 8 <= n && line[i] == 'i' && line[i + 1] == 'n' && line[i + 2] == 'c' &&
      line[i + 3] == 'l' && line[i + 4] == 'u' && line[i + 5] == 'd' && line[i + 6] == 'e' &&
      line[i + 7] == 'c') {
    j = i + 8;
    while (j < n && (line[j] == ' ' || line[j] == 9))
      j++;
    if (j < n && line[j] == 34)
      return 2;
  }
  return 0;
}
static BASALT_UNUSED void *basalt_include_open_root(const char *path) {
  char *p = basalt_inc_realpath(path);
  FILE *f;
  basalt_inc_last_path = p;
  basalt_inc_project_root = basalt_inc_cwd();
  if (!basalt_inc_begin(p))
    return NULL;
  f = fopen(p, (const char[]){114, 0});
  if (!f) {
    basalt_inc_status = 3;
    if (basalt_inc_active_n)
      basalt_inc_active_n--;
    return NULL;
  }
  return (void *)f;
}
static BASALT_UNUSED void *basalt_include_open_line(int *line, int n, int mode) {
  int i = 0, a, b, j;
  char *raw, *joined, *canon, *expanded;
  FILE *f;
  (void)mode;
  basalt_inc_last_path = basalt_inc_strdup("");
  while (i < n && line[i] != 34)
    i++;
  if (i >= n)
    return NULL;
  a = ++i;
  while (i < n && line[i] != 34)
    i++;
  if (i >= n)
    return NULL;
  b = i;
  raw = (char *)malloc((size_t)(b - a) + 1);
  if (!raw)
    exit(2);
  {
    int k;
    for (k = 0; k < b - a; k++)
      raw[k] = (char)line[a + k];
  }
  raw[b - a] = 0;
  raw = (char *)basalt_track(raw);
  j = i + 1;
  while (j < n && (line[j] == 32 || line[j] == 9))
    j++;
  if (j < n && line[j] == 59)
    j++;
  while (j < n && (line[j] == 32 || line[j] == 9))
    j++;
  if (j != n)
    return NULL;
  expanded = basalt_inc_expand_prefix(raw);
  if (expanded) {
    if (!basalt_inc_prefix_safe(raw)) {
      basalt_inc_status = 4;
      return NULL;
    }
    joined = basalt_inc_dir_join(basalt_inc_project_root, expanded);
    canon = basalt_inc_realpath(joined);
  } else {
    joined = basalt_inc_join(basalt_inc_active[basalt_inc_active_n - 1], raw);
    canon = basalt_inc_realpath(joined);
  }
  basalt_inc_last_path = canon;
  if (!basalt_inc_begin(canon))
    return NULL;
  f = fopen(canon, (const char[]){114, 0});
  if (!f) {
    basalt_inc_status = 3;
    if (basalt_inc_active_n)
      basalt_inc_active_n--;
    return NULL;
  }
  return (void *)f;
}
static BASALT_UNUSED int basalt_include_last_status(void) {
  return basalt_inc_status;
}
static BASALT_UNUSED void basalt_include_reset_session(void) {
  basalt_inc_active_n = 0;
  basalt_inc_loaded_n = 0;
  basalt_inc_last_path = NULL;
  basalt_inc_project_root = NULL;
  basalt_inc_status = 0;
}
static BASALT_UNUSED void *open_file(const char *p, const char *m) {
  return (void *)fopen(p, m);
}
static BASALT_UNUSED int read_char(void *h) {
  int c = fgetc((FILE *)h);
  return c == EOF ? -1 : c;
}
static BASALT_UNUSED int close_file(void *h) {
  return fclose((FILE *)h);
}
static BASALT_UNUSED int write_char(void *h, int c) {
  return fputc(c, (FILE *)h);
}
static BASALT_UNUSED int write_string(void *h, const char *s) {
  return fputs(s, (FILE *)h);
}
static void **basalt_live = NULL;
static size_t basalt_live_n = 0, basalt_live_cap = 0;
static BASALT_UNUSED size_t basalt_find(void *p) {
  size_t i;
  for (i = 0; i < basalt_live_n; i++)
    if (basalt_live[i] == p)
      return i;
  return (size_t)-1;
}
static BASALT_UNUSED void basalt_validate(void) {
  size_t i, j;
  for (i = 0; i < basalt_live_n; i++) {
    if (!basalt_live[i])
      basalt_panic(2);
    for (j = i + 1; j < basalt_live_n; j++)
      if (basalt_live[i] == basalt_live[j])
        basalt_panic(2);
  }
}
static BASALT_UNUSED void basalt_cleanup(void) {
  size_t i;
  basalt_validate();
  for (i = 0; i < basalt_live_n; i++)
    free(basalt_live[i]);
  free(basalt_live);
  basalt_live = NULL;
  basalt_live_n = basalt_live_cap = 0;
}
static BASALT_UNUSED void *basalt_track(void *p) {
  size_t c;
  void **q;
  if (!p)
    return NULL;
  if (basalt_find(p) != (size_t)-1)
    basalt_panic(2);
  if (basalt_live_n == basalt_live_cap) {
    if (basalt_live_cap > (size_t)-1 / 2)
      c = (size_t)-1;
    else
      c = basalt_live_cap ? basalt_live_cap * 2 : 32;
    if (c > (size_t)-1 / sizeof(void *))
      basalt_panic(2);
    q = (void **)realloc(basalt_live, c * sizeof(void *));
    if (!q)
      basalt_panic(2);
    basalt_live = q;
    basalt_live_cap = c;
  }
  basalt_live[basalt_live_n++] = p;
  atexit(basalt_cleanup);
  return p;
}
static BASALT_UNUSED void basalt_release(void *p) {
  size_t i;
  if (!p)
    return;
  i = basalt_find(p);
  if (i == (size_t)-1)
    basalt_panic(2);
  free(p);
  basalt_live[i] = basalt_live[--basalt_live_n];
}
static int basalt_io_status = 0;
static BASALT_UNUSED int runtime_io_status(void) {
  return basalt_io_status;
}
static BASALT_UNUSED char *runtime_read_line(int max_len) {
  size_t n = 0;
  int c = EOF;
  char *p;
  if (max_len < 2 || max_len > 1048576)
    basalt_panic(7);
  p = (char *)malloc((size_t)max_len);
  if (!p)
    basalt_panic(5);
  while (n + 1 < (size_t)max_len) {
    c = fgetc(stdin);
    if (c == EOF)
      break;
    if (c == '\n')
      break;
    p[n++] = (char)c;
  }
  p[n] = 0;
  if (c != EOF && c != '\n' && n + 1 == (size_t)max_len) {
    basalt_io_status = 3;
    do {
      c = fgetc(stdin);
    } while (c != EOF && c != '\n');
  } else if (c == EOF && n == 0)
    basalt_io_status = 1;
  else
    basalt_io_status = 0;
  return (char *)basalt_track(p);
}
static BASALT_UNUSED int runtime_read_int(int fallback) {
  char buf[128];
  size_t n = 0;
  int c = EOF;
  char *end;
  long v;
  while (n + 1 < sizeof(buf)) {
    c = fgetc(stdin);
    if (c == EOF || c == '\n')
      break;
    if (c != '\r')
      buf[n++] = (char)c;
  }
  buf[n] = 0;
  if (c != EOF && c != '\n' && n + 1 == sizeof(buf)) {
    basalt_io_status = 3;
    do {
      c = fgetc(stdin);
    } while (c != EOF && c != '\n');
    return fallback;
  }
  if (c == EOF && n == 0) {
    basalt_io_status = 1;
    return fallback;
  }
  errno = 0;
  v = strtol(buf, &end, 10);
  while (*end == ' ' || *end == '\t' || *end == '\r')
    end++;
  if (end == buf || *end != 0) {
    basalt_io_status = 2;
    return fallback;
  }
  if (errno == ERANGE || v < (long)INT_MIN || v > (long)INT_MAX) {
    basalt_io_status = 4;
    return fallback;
  }
  basalt_io_status = 0;
  return (int)v;
}
static BASALT_UNUSED void runtime_write_string(const char *s) {
  if (!s)
    basalt_panic(4);
  if (fputs(s, stdout) == EOF || fflush(stdout) != 0)
    basalt_panic(8);
}
static BASALT_UNUSED void runtime_write_line(const char *s) {
  if (!s)
    basalt_panic(4);
  if (fputs(s, stdout) == EOF || fputc('\n', stdout) == EOF || fflush(stdout) != 0)
    basalt_panic(8);
}
static BASALT_UNUSED void runtime_write_int(int value) {
  if (fprintf(stdout, "%d", value) < 0 || fflush(stdout) != 0)
    basalt_panic(8);
}
static BASALT_UNUSED void runtime_write_char(char value) {
  if (fputc((unsigned char)value, stdout) == EOF || fflush(stdout) != 0)
    basalt_panic(8);
}
static BASALT_UNUSED void *basalt_memory_alloc(int count, size_t elem_size) {
  size_t bytes = basalt_checked_bytes(count, elem_size);
  void *p = calloc(1, bytes ? bytes : 1);
  if (!p)
    basalt_panic(5);
  return basalt_track(p);
}
static BASALT_UNUSED void *basalt_memory_alloc_aligned(int count, int alignment, size_t elem_size) {
  size_t bytes, rounded, a;
  void *p;
  if (count < 0 || alignment < 1)
    basalt_panic(1);
  a = (size_t)alignment;
  if ((a & (a - 1)) != 0)
    basalt_panic(1);
  if (a < sizeof(void *))
    a = sizeof(void *);
  bytes = basalt_checked_bytes(count, elem_size);
  if (bytes == 0)
    bytes = 1;
  if (bytes > (size_t)-1 - (a - 1))
    basalt_panic(1);
  rounded = (bytes + a - 1) & ~(a - 1);
  p = aligned_alloc(a, rounded);
  if (!p)
    basalt_panic(5);
  return basalt_track(p);
}
static BASALT_UNUSED void *basalt_memory_resize(void *old, int old_count, int new_count,
                                                size_t elem_size) {
  size_t slot = (size_t)-1;
  size_t old_bytes;
  size_t new_bytes;
  void *p;
  if (old_count < 0 || new_count < 0 || new_count < old_count)
    basalt_panic(1);
  if (old) {
    slot = basalt_find(old);
    if (slot == (size_t)-1)
      basalt_panic(2);
  }
  old_bytes = basalt_checked_bytes(old_count, elem_size);
  new_bytes = basalt_checked_bytes(new_count, elem_size);
  p = realloc(old, new_bytes ? new_bytes : 1);
  if (!p)
    basalt_panic(6);
  if (slot == (size_t)-1)
    basalt_track(p);
  else
    basalt_live[slot] = p;
  if (new_bytes > old_bytes)
    memset((char *)p + old_bytes, 0, new_bytes - old_bytes);
  return p;
}
static BASALT_UNUSED void basalt_memory_free(void *p) {
  basalt_release(p);
}
static BASALT_UNUSED char *runtime_string_concat(const char *a, const char *b) {
  size_t na, nb, total;
  char *p;
  if (!a || !b)
    basalt_panic(4);
  na = strlen(a);
  nb = strlen(b);
  if (na > (size_t)-1 - nb - 1)
    basalt_panic(1);
  total = na + nb + 1;
  p = (char *)malloc(total);
  if (!p)
    basalt_panic(5);
  memcpy(p, a, na);
  memcpy(p + na, b, nb);
  p[na + nb] = 0;
  return (char *)basalt_track(p);
}
static char *basalt_sys_out = NULL;
static char *basalt_sys_err = NULL;
static int basalt_sys_status_value = 0;
static int basalt_sys_ok_value = 0;
static int basalt_sys_truncated_value = 0;
static int basalt_sys_spawn_error_value = 0;
static int basalt_sys_cleanup_registered = 0;
static void basalt_sys_free_buffers(void) {
  if (basalt_sys_out) {
    free(basalt_sys_out);
    basalt_sys_out = NULL;
  }
  if (basalt_sys_err) {
    free(basalt_sys_err);
    basalt_sys_err = NULL;
  }
}
static BASALT_UNUSED char *basalt_sys_empty(void) {
  char *p = (char *)malloc(1);
  if (!p)
    basalt_panic(5);
  p[0] = 0;
  return (char *)basalt_track(p);
}
static BASALT_UNUSED char *basalt_sys_raw_empty(void) {
  char *p = (char *)malloc(1);
  if (!p)
    basalt_panic(5);
  p[0] = 0;
  return p;
}
static BASALT_UNUSED char *basalt_sys_copy_n(const char *s, size_t n) {
  char *p = (char *)malloc(n + 1);
  if (!p)
    basalt_panic(5);
  if (n)
    memcpy(p, s, n);
  p[n] = 0;
  return (char *)basalt_track(p);
}
#if defined(_WIN32)
static BASALT_UNUSED void basalt_sys_drain(int fd, char *buf, size_t cap, size_t *len,
                                           int *truncated, int *open_flag) {
  (void)fd;
  (void)buf;
  (void)cap;
  (void)len;
  (void)truncated;
  (void)open_flag;
}
#else
static BASALT_UNUSED void basalt_sys_drain(int fd, char *buf, size_t cap, size_t *len,
                                           int *truncated, int *open_flag) {
  char chunk[4096];
  ssize_t n;
  while ((n = read(fd, chunk, sizeof(chunk))) > 0) {
    size_t take = 0;
    if (*len < cap) {
      take = (size_t)n;
      if (take > cap - *len)
        take = cap - *len;
      if (take)
        memcpy(buf + *len, chunk, take);
      *len += take;
    }
    if ((size_t)n > take)
      *truncated = 1;
  }
  if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
    close(fd);
    *open_flag = 0;
  }
}
#endif
#if defined(_WIN32)
typedef struct basalt_sys_windows_reader {
  HANDLE pipe;
  char *buffer;
  size_t cap;
  size_t len;
  int truncated;
  DWORD error;
} basalt_sys_windows_reader;
static BASALT_UNUSED int basalt_sys_windows_error(DWORD value) {
  if (value == 0)
    return 4;
  if (value > (DWORD)INT_MAX)
    return INT_MAX;
  return (int)value;
}
static BASALT_UNUSED int basalt_sys_utf8_to_wide(const char *value, wchar_t **result) {
  int needed;
  wchar_t *buffer;
  if (!value || !result)
    return 0;
  needed = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, NULL, 0);
  if (needed <= 0)
    return 0;
  buffer = (wchar_t *)malloc((size_t)needed * sizeof(*buffer));
  if (!buffer)
    basalt_panic(5);
  if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, buffer, needed) != needed) {
    free(buffer);
    return 0;
  }
  *result = buffer;
  return 1;
}
static BASALT_UNUSED int basalt_sys_size_add(size_t *value, size_t add) {
  if (add > (size_t)-1 - *value)
    return 0;
  *value += add;
  return 1;
}
static BASALT_UNUSED int basalt_sys_quote_len(const wchar_t *value, size_t *result) {
  size_t total = 2;
  size_t slashes = 0;
  const wchar_t *p;
  if (!value || !result)
    return 0;
  for (p = value; *p != L'\0'; p++) {
    if (*p == L'\\') {
      if (slashes == (size_t)-1)
        return 0;
      slashes++;
    } else {
      size_t add = slashes;
      if (*p == L'"') {
        if (slashes > ((size_t)-2) / 2)
          return 0;
        add = slashes * 2 + 1;
      }
      if (!basalt_sys_size_add(&total, add))
        return 0;
      slashes = 0;
    }
  }
  if (slashes > ((size_t)-1) / 2)
    return 0;
  if (!basalt_sys_size_add(&total, slashes * 2))
    return 0;
  *result = total;
  return 1;
}
static BASALT_UNUSED void basalt_sys_quote_append(const wchar_t *value, wchar_t *out, size_t *pos) {
  size_t slashes = 0;
  const wchar_t *p;
  out[(*pos)++] = L'"';
  for (p = value; *p != L'\0'; p++) {
    if (*p == L'\\') {
      slashes++;
    } else {
      size_t count = slashes;
      if (*p == L'"')
        count = slashes * 2 + 1;
      while (count > 0) {
        out[(*pos)++] = L'\\';
        count--;
      }
      out[(*pos)++] = *p;
      slashes = 0;
    }
  }
  while (slashes > 0) {
    out[(*pos)++] = L'\\';
    out[(*pos)++] = L'\\';
    slashes--;
  }
  out[(*pos)++] = L'"';
}
static DWORD WINAPI basalt_sys_windows_reader_thread(LPVOID raw) {
  basalt_sys_windows_reader *reader = (basalt_sys_windows_reader *)raw;
  char chunk[4096];
  DWORD count;
  BOOL ok;
  for (;;) {
    count = 0;
    ok = ReadFile(reader->pipe, chunk, (DWORD)sizeof(chunk), &count, NULL);
    if (!ok) {
      DWORD error = GetLastError();
      if (error != ERROR_BROKEN_PIPE)
        reader->error = error;
      break;
    }
    if (count == 0)
      break;
    {
      size_t take = 0;
      if (reader->len < reader->cap) {
        take = (size_t)count;
        if (take > reader->cap - reader->len)
          take = reader->cap - reader->len;
        if (take)
          memcpy(reader->buffer + reader->len, chunk, take);
        reader->len += take;
      }
      if ((size_t)count > take)
        reader->truncated = 1;
    }
  }
  CloseHandle(reader->pipe);
  reader->pipe = NULL;
  return 0;
}
#endif
#if defined(_WIN32)
static BASALT_UNUSED int basalt_sys_windows_run(const char *executable, char **args, int arg_count,
                                                size_t cap, char *out, char *err, int *truncated,
                                                int *spawn_error) {
  HANDLE out_read = NULL, out_write = NULL, err_read = NULL, err_write = NULL, stdin_child = NULL;
  int stdin_owned = 0;
  SECURITY_ATTRIBUTES security;
  STARTUPINFOEXW startup;
  PROCESS_INFORMATION process;
  wchar_t *exe_wide = NULL;
  wchar_t **wide_args = NULL;
  wchar_t *command_line = NULL;
  size_t command_length = 0, item_length = 0, pos = 0;
  int i;
  int result = -1;
  int process_created = 0;
  DWORD wait_result = WAIT_FAILED;
  DWORD exit_code = 1;
  DWORD attribute_error = 0;
  DWORD thread_error = 0;
  SIZE_T attribute_size = 0;
  LPPROC_THREAD_ATTRIBUTE_LIST attribute_list = NULL;
  int attribute_initialized = 0;
  basalt_sys_windows_reader readers[2];
  HANDLE threads[2] = {NULL, NULL};
  HANDLE inherited_handles[3] = {NULL, NULL, NULL};
  const wchar_t nul_name[4] = {78, 85, 76, 0};
  memset(&security, 0, sizeof(security));
  security.nLength = (DWORD)sizeof(security);
  security.bInheritHandle = TRUE;
  memset(&startup, 0, sizeof(startup));
  memset(&process, 0, sizeof(process));
  memset(readers, 0, sizeof(readers));
  memset(inherited_handles, 0, sizeof(inherited_handles));
  if (!CreatePipe(&out_read, &out_write, &security, 0) ||
      !SetHandleInformation(out_read, HANDLE_FLAG_INHERIT, 0)) {
    goto windows_cleanup;
  }
  if (!CreatePipe(&err_read, &err_write, &security, 0) ||
      !SetHandleInformation(err_read, HANDLE_FLAG_INHERIT, 0)) {
    goto windows_cleanup;
  }
  {
    HANDLE current_input = GetStdHandle(STD_INPUT_HANDLE);
    if (current_input != NULL && current_input != INVALID_HANDLE_VALUE) {
      if (!DuplicateHandle(GetCurrentProcess(), current_input, GetCurrentProcess(), &stdin_child, 0,
                           TRUE, DUPLICATE_SAME_ACCESS))
        stdin_child = NULL;
    }
    if (stdin_child == NULL) {
      stdin_child = CreateFileW(nul_name, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                                &security, OPEN_EXISTING, 0, NULL);
      if (stdin_child == INVALID_HANDLE_VALUE)
        stdin_child = NULL;
      if (stdin_child == NULL)
        goto windows_cleanup;
    }
    stdin_owned = 1;
  }
  if (!basalt_sys_utf8_to_wide(executable, &exe_wide)) {
    *spawn_error = ERROR_NO_UNICODE_TRANSLATION;
    goto windows_cleanup;
  }
  if (!basalt_sys_quote_len(exe_wide, &item_length) ||
      !basalt_sys_size_add(&command_length, item_length)) {
    *spawn_error = ERROR_NOT_ENOUGH_MEMORY;
    goto windows_cleanup;
  }
  if (arg_count > 0) {
    wide_args = (wchar_t **)calloc((size_t)arg_count, sizeof(*wide_args));
    if (!wide_args)
      basalt_panic(5);
  }
  for (i = 0; i < arg_count; i++) {
    if (!args || !args[i] || !basalt_sys_utf8_to_wide(args[i], &wide_args[i])) {
      *spawn_error = ERROR_NO_UNICODE_TRANSLATION;
      goto windows_cleanup;
    }
    if (!basalt_sys_size_add(&command_length, 1) ||
        !basalt_sys_quote_len(wide_args[i], &item_length) ||
        !basalt_sys_size_add(&command_length, item_length)) {
      *spawn_error = ERROR_NOT_ENOUGH_MEMORY;
      goto windows_cleanup;
    }
  }
  if (command_length > (size_t)-1 / sizeof(wchar_t) - 1) {
    *spawn_error = ERROR_NOT_ENOUGH_MEMORY;
    goto windows_cleanup;
  }
  command_line = (wchar_t *)malloc((command_length + 1) * sizeof(*command_line));
  if (!command_line)
    basalt_panic(5);
  basalt_sys_quote_append(exe_wide, command_line, &pos);
  for (i = 0; i < arg_count; i++) {
    command_line[pos++] = L' ';
    basalt_sys_quote_append(wide_args[i], command_line, &pos);
  }
  command_line[pos] = L'\0';
  inherited_handles[0] = stdin_child;
  inherited_handles[1] = out_write;
  inherited_handles[2] = err_write;
  if (InitializeProcThreadAttributeList(NULL, 1, 0, &attribute_size)) {
    *spawn_error = 4;
    goto windows_cleanup;
  }
  attribute_error = GetLastError();
  if (attribute_error != ERROR_INSUFFICIENT_BUFFER || attribute_size == 0) {
    *spawn_error = basalt_sys_windows_error(attribute_error);
    goto windows_cleanup;
  }
  attribute_list = (LPPROC_THREAD_ATTRIBUTE_LIST)malloc(attribute_size);
  if (!attribute_list)
    basalt_panic(5);
  if (!InitializeProcThreadAttributeList(attribute_list, 1, 0, &attribute_size)) {
    *spawn_error = basalt_sys_windows_error(GetLastError());
    goto windows_cleanup;
  }
  attribute_initialized = 1;
  if (!UpdateProcThreadAttribute(attribute_list, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                                 inherited_handles, (SIZE_T)(3 * sizeof(inherited_handles[0])),
                                 NULL, NULL)) {
    *spawn_error = basalt_sys_windows_error(GetLastError());
    goto windows_cleanup;
  }
  startup.StartupInfo.cb = (DWORD)sizeof(startup);
  startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
  startup.StartupInfo.hStdInput = stdin_child;
  startup.StartupInfo.hStdOutput = out_write;
  startup.StartupInfo.hStdError = err_write;
  startup.lpAttributeList = attribute_list;
  if (!CreateProcessW(NULL, command_line, NULL, NULL, TRUE,
                      CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT, NULL, NULL,
                      &startup.StartupInfo, &process)) {
    *spawn_error = basalt_sys_windows_error(GetLastError());
    goto windows_cleanup;
  }
  process_created = 1;
  CloseHandle(out_write);
  out_write = NULL;
  CloseHandle(err_write);
  err_write = NULL;
  CloseHandle(stdin_child);
  stdin_child = NULL;
  readers[0].pipe = out_read;
  readers[0].buffer = out;
  readers[0].cap = cap;
  readers[0].len = 0;
  readers[0].truncated = 0;
  readers[0].error = 0;
  readers[1].pipe = err_read;
  readers[1].buffer = err;
  readers[1].cap = cap;
  readers[1].len = 0;
  readers[1].truncated = 0;
  readers[1].error = 0;
  threads[0] = CreateThread(NULL, 0, basalt_sys_windows_reader_thread, &readers[0], 0, NULL);
  if (!threads[0]) {
    thread_error = GetLastError();
    CloseHandle(out_read);
    out_read = NULL;
    readers[0].pipe = NULL;
  }
  threads[1] = CreateThread(NULL, 0, basalt_sys_windows_reader_thread, &readers[1], 0, NULL);
  if (!threads[1]) {
    if (thread_error == 0)
      thread_error = GetLastError();
    CloseHandle(err_read);
    err_read = NULL;
    readers[1].pipe = NULL;
  }
  if (thread_error != 0) {
    *spawn_error = basalt_sys_windows_error(thread_error);
    TerminateProcess(process.hProcess, 1);
  }
  wait_result = WaitForSingleObject(process.hProcess, INFINITE);
  if (wait_result != WAIT_OBJECT_0 && *spawn_error == 0)
    *spawn_error = basalt_sys_windows_error(GetLastError());
  if (threads[0]) {
    WaitForSingleObject(threads[0], INFINITE);
    CloseHandle(threads[0]);
    threads[0] = NULL;
    out_read = NULL;
  }
  if (threads[1]) {
    WaitForSingleObject(threads[1], INFINITE);
    CloseHandle(threads[1]);
    threads[1] = NULL;
    err_read = NULL;
  }
  if (readers[0].error != 0 && *spawn_error == 0)
    *spawn_error = basalt_sys_windows_error(readers[0].error);
  if (readers[1].error != 0 && *spawn_error == 0)
    *spawn_error = basalt_sys_windows_error(readers[1].error);
  *truncated = readers[0].truncated || readers[1].truncated;
  out[readers[0].len] = 0;
  err[readers[1].len] = 0;
  if (*spawn_error == 0 && !GetExitCodeProcess(process.hProcess, &exit_code))
    *spawn_error = basalt_sys_windows_error(GetLastError());
  if (*spawn_error == 0) {
    if (exit_code > (DWORD)INT_MAX)
      result = INT_MAX;
    else
      result = (int)exit_code;
  }
  CloseHandle(process.hThread);
  CloseHandle(process.hProcess);
  process_created = 0;
windows_cleanup:
  basalt_sys_status_value = result;
  basalt_sys_ok_value = (result == 0 && *spawn_error == 0);
  if (process_created) {
    TerminateProcess(process.hProcess, 1);
    WaitForSingleObject(process.hProcess, INFINITE);
    CloseHandle(process.hThread);
    CloseHandle(process.hProcess);
  }
  if (attribute_initialized) {
    DeleteProcThreadAttributeList(attribute_list);
    attribute_initialized = 0;
  }
  if (attribute_list) {
    free(attribute_list);
    attribute_list = NULL;
  }
  if (threads[0])
    CloseHandle(threads[0]);
  if (threads[1])
    CloseHandle(threads[1]);
  if (out_read)
    CloseHandle(out_read);
  if (out_write)
    CloseHandle(out_write);
  if (err_read)
    CloseHandle(err_read);
  if (err_write)
    CloseHandle(err_write);
  if (stdin_owned && stdin_child)
    CloseHandle(stdin_child);
  if (command_line)
    free(command_line);
  if (exe_wide)
    free(exe_wide);
  if (wide_args) {
    for (i = 0; i < arg_count; i++)
      if (wide_args[i])
        free(wide_args[i]);
    free(wide_args);
  }
  return result;
}
#endif
int basalt_sys_run(const char *executable, char **args, int arg_count, int max_output) {
  size_t cap;
#if !defined(_WIN32)
  size_t out_len = 0, err_len = 0;
  int i;
#endif
  basalt_sys_free_buffers();
  if (!basalt_sys_cleanup_registered) {
    atexit(basalt_sys_free_buffers);
    basalt_sys_cleanup_registered = 1;
  }
  basalt_sys_status_value = -1;
  basalt_sys_ok_value = 0;
  basalt_sys_truncated_value = 0;
  basalt_sys_spawn_error_value = 0;
  if (!executable || arg_count < 0 || max_output < 0 || max_output > 16777216) {
    basalt_sys_spawn_error_value = EINVAL;
    basalt_sys_out = basalt_sys_raw_empty();
    basalt_sys_err = basalt_sys_raw_empty();
    return -1;
  }
  cap = (size_t)max_output;
  basalt_sys_out = (char *)malloc(cap + 1);
  basalt_sys_err = (char *)malloc(cap + 1);
  if (!basalt_sys_out || !basalt_sys_err)
    basalt_panic(5);
  basalt_sys_out[0] = 0;
  basalt_sys_err[0] = 0;
#if defined(_WIN32)
  return basalt_sys_windows_run(executable, args, arg_count, cap, basalt_sys_out, basalt_sys_err,
                                &basalt_sys_truncated_value, &basalt_sys_spawn_error_value);
#else
  {
    int out_pipe[2], err_pipe[2], exec_pipe[2];
    pid_t child;
    int status;
    int out_open = 1, err_open = 1;
    int exec_error = 0;
    ssize_t exec_read;
    if (pipe(out_pipe) != 0 || pipe(err_pipe) != 0 || pipe(exec_pipe) != 0) {
      basalt_sys_spawn_error_value = errno;
      return -1;
    }
    if (fcntl(exec_pipe[1], F_SETFD, FD_CLOEXEC) < 0) {
      basalt_sys_spawn_error_value = errno;
      close(out_pipe[0]);
      close(out_pipe[1]);
      close(err_pipe[0]);
      close(err_pipe[1]);
      close(exec_pipe[0]);
      close(exec_pipe[1]);
      return -1;
    }
    child = fork();
    if (child < 0) {
      basalt_sys_spawn_error_value = errno;
      close(out_pipe[0]);
      close(out_pipe[1]);
      close(err_pipe[0]);
      close(err_pipe[1]);
      close(exec_pipe[0]);
      close(exec_pipe[1]);
      basalt_sys_out = basalt_sys_raw_empty();
      basalt_sys_err = basalt_sys_raw_empty();
      return -1;
    }
    if (child == 0) {
      char **av = (char **)calloc((size_t)arg_count + 2, sizeof(char *));
      if (!av)
        _exit(127);
      av[0] = (char *)executable;
      for (i = 0; i < arg_count; i++)
        av[i + 1] = args[i];
      av[arg_count + 1] = NULL;
      close(out_pipe[0]);
      close(err_pipe[0]);
      close(exec_pipe[0]);
      if (dup2(out_pipe[1], STDOUT_FILENO) < 0 || dup2(err_pipe[1], STDERR_FILENO) < 0) {
        int e = errno;
        if (write(exec_pipe[1], &e, sizeof(e)) < 0) {
        }
        _exit(127);
      }
      close(out_pipe[1]);
      close(err_pipe[1]);
      execvp(executable, av);
      {
        int e = errno;
        if (write(exec_pipe[1], &e, sizeof(e)) < 0) {
        }
      }
      _exit(127);
    }
    close(out_pipe[1]);
    close(err_pipe[1]);
    close(exec_pipe[1]);
    if (fcntl(out_pipe[0], F_SETFL, O_NONBLOCK) < 0 ||
        fcntl(err_pipe[0], F_SETFL, O_NONBLOCK) < 0) {
      basalt_sys_spawn_error_value = errno;
    }
    while (out_open || err_open) {
      struct pollfd pf[2];
      int polled;
      pf[0].fd = out_open ? out_pipe[0] : -1;
      pf[0].events = POLLIN;
      pf[1].fd = err_open ? err_pipe[0] : -1;
      pf[1].events = POLLIN;
      polled = poll(pf, 2, -1);
      if (polled < 0) {
        if (errno == EINTR)
          continue;
        break;
      }
      if (out_open)
        basalt_sys_drain(out_pipe[0], basalt_sys_out, cap, &out_len, &basalt_sys_truncated_value,
                         &out_open);
      if (err_open)
        basalt_sys_drain(err_pipe[0], basalt_sys_err, cap, &err_len, &basalt_sys_truncated_value,
                         &err_open);
    }
    if (out_open) {
      close(out_pipe[0]);
      out_open = 0;
    }
    if (err_open) {
      close(err_pipe[0]);
      err_open = 0;
    }
    waitpid(child, &status, 0);
    exec_read = read(exec_pipe[0], &exec_error, sizeof(exec_error));
    close(exec_pipe[0]);
    basalt_sys_out[out_len] = 0;
    basalt_sys_err[err_len] = 0;
    if (exec_read > 0) {
      basalt_sys_spawn_error_value = exec_error;
      basalt_sys_status_value = -1;
    } else if (WIFEXITED(status))
      basalt_sys_status_value = WEXITSTATUS(status);
    else if (WIFSIGNALED(status))
      basalt_sys_status_value = 0 - WTERMSIG(status);
    else
      basalt_sys_status_value = -1;
    basalt_sys_ok_value = (basalt_sys_status_value == 0 && basalt_sys_spawn_error_value == 0);
    return basalt_sys_status_value;
  }
#endif
}
int basalt_sys_run_status(void) {
  return basalt_sys_ok_value;
}
char *basalt_sys_stdout(void) {
  return basalt_sys_out ? basalt_sys_out : basalt_sys_empty();
}
char *basalt_sys_stderr(void) {
  return basalt_sys_err ? basalt_sys_err : basalt_sys_empty();
}
int basalt_sys_truncated(void) {
  return basalt_sys_truncated_value;
}
int basalt_sys_spawn_error(void) {
  return basalt_sys_spawn_error_value;
}
int basalt_compile_argv(const char *cc, const char *input_c, const char *output_bin, char **all,
                        int start, int extra_count) {
  char **av;
  int i, status;
  if (!cc || !input_c || !output_bin || !all || start < 0 || extra_count < 0)
    return -1;
  av = (char **)calloc((size_t)extra_count + 4, sizeof(char *));
  if (!av)
    basalt_panic(5);
  for (i = 0; i < extra_count; i++)
    av[i] = all[start + i];
  av[extra_count] = (char *)input_c;
  av[extra_count + 1] = (char *)"-o";
  av[extra_count + 2] = (char *)output_bin;
  av[extra_count + 3] = NULL;
  status = basalt_sys_run(cc, av, extra_count + 3, 65536);
  if (status != 0 && basalt_sys_stderr()[0] != 0)
    fputs(basalt_sys_stderr(), stderr);
  free(av);
  return status;
}
static BASALT_UNUSED int *alloc_ints(int n) {
  int *p;
  if (n < 0)
    basalt_panic(1);
  if (n < 1)
    n = 1;
  basalt_checked_bytes(n, sizeof(int));
  p = (int *)calloc((size_t)n, sizeof(int));
  if (!p)
    basalt_panic(5);
  return (int *)basalt_track(p);
}
static BASALT_UNUSED void free_ints(int *p) {
  basalt_release(p);
}
static BASALT_UNUSED int *grow_ints(int *p, int old, int n) {
  size_t slot = (size_t)-1;
  int *q;
  if (old < 0 || n < 0)
    basalt_panic(1);
  if (n <= old)
    return p;
  if (p) {
    slot = basalt_find(p);
    if (slot == (size_t)-1)
      basalt_panic(2);
  }
  basalt_checked_bytes(n, sizeof(int));
  q = (int *)realloc(p, (size_t)n * sizeof(int));
  if (!q)
    basalt_panic(6);
  if (p)
    basalt_live[slot] = q;
  else
    basalt_track(q);
  memset(q + old, 0, (size_t)(n - old) * sizeof(int));
  return q;
}
extern int *payload_int;
extern int *payload_name;
extern int *payload_string;
extern int *code_kind;
extern int *code_value;
extern int *input_kind;
extern int *input_value;
extern int *source;
extern int *sym_start;
extern int *sym_len;
extern int *sym_hash;
extern int *sym_kind;
extern int *sym_type;
extern int *sym_scope;

int next_capacity(int old, int need);
void ensure_node(int need);
void ensure_payload(int need);
void ensure_code(int need);
void ensure_input(int need);
void ensure_source(int need);
void ensure_sym(int need);
void ensure_ffi_headers(int need);
int ffi_header_char_ok(int c);
int ffi_header_valid(int id);
int ffi_header_seen(int id);
void ffi_header_register(int id);
int ast_node(int kind, int a, int b, int c, int value, int aux);
int ast_link(int head, int item);
int payload_make_int(int value);
int payload_make_name(int name_id);
int payload_make_string(int string_id);
void ensure_snapshot(int need);
void gen_bind_clear(void);
void ensure_gen_bind(int need);
int gen_bind_find(int name);
void gen_bind_add(int name, int ty);
void ensure_gen_tuple(int need);
void ensure_gen_closure(int need);
int gen_closure_signature(int id);
int gen_closure_name(int serial, char *prefix);
int gen_closure_env_name(int serial);
int gen_closure_invoke_name(int serial);
int gen_closure_factory_name(int serial);
int gen_closure_value_name(int serial);
void gen_closure_register(int id);
void gen_append_char(int c);
void gen_append_char_text(char c);
void gen_append_text(char *text);
void gen_append_symbol(int id);
void gen_append_c_symbol(int id);
int gen_mangle_intern(int kind);
int sym_c_symbol(int id);
void gen_append_uint(int value);
int gen_tuple_field_name(int index);
void gen_mangle_type(int ty);
int gen_mangled_type_symbol(int ty);
void gen_add_tuple_type(int ty);
int gen_mangled_function_symbol(int base, int args);
void code_emit(int kind, int value);
void code_reset(void);
void ensure_emit_defer(int need);
void ensure_emit_scope(int need);
void ensure_emit_loop(int need);
void gen_defer_push(int expr);
void gen_emit_defer_from(int base);
void gen_emit_all_defers(void);
void ensure_gen_specs(int need);
void ensure_gen_struct_state(int need);
void ensure_gen_spec_state(int need);
int gen_find_spec_index(int decl, int name);
int gen_substitute_type(int ty);
int gen_active_closure_capture(int name);
int gen_closure_env_param_name(int serial);
int gen_closure_env_local_name(int serial);
int gen_closure_active_env_name(void);
int gen_closure_arg_name(int index);
int gen_closure_call_name(int sig);
int gen_active_param_type(int name);
int gen_local_decl_type(int id, int name);
int gen_active_local_decl_type(int name);
int gen_spec_exists(int kind, int decl, int name);
void gen_collect_struct_fields(int decl, int inst);
void gen_add_struct_spec(int ty);
int gen_type_has_param(int ty);
void gen_add_fun_spec(int decl, int args);
void gen_collect_type(int ty);
void gen_collect_expr(int id);
void gen_collect_stmt(int id);
void gen_alignment(int alignment);
void gen_primitive_type(int kind);
void gen_type(int kind, int child, int size);
int gen_scalar_kind(int arg);
int gen_scalar_name(int arg);
int gen_array_elem_kind(int arg);
int gen_array_elem_name(int arg);
void gen_array_elem_type(int kind, int name);
void gen_array_sizeof(int kind, int name);
void gen_array_value_ptr(int kind, int name, int value);
void gen_array_sizeof_node(int ty);
void gen_memory_sizeof(int arg);
void gen_memory_builtin(int id);
int gen_call_name(int id);
void gen_variant_expr(int id);
void gen_expr(int id);
void gen_expr_condition_inner(int id);
int gen_expr_kind(int id);
void gen_initializer(int ty, int expr);
void gen_assignment(int lhs, int rhs);
int compound_c_operator(int op);
void gen_compound_assignment(int lhs, int op, int rhs);
void gen_for_clause(int id);
void ensure_gen_fun_chain(int need);
void gen_fun_decl(int ty, int name);
void gen_const_decl(int ty, int name);
void gen_array_base_type(int ty);
void gen_array_suffixes(int ty);
void gen_decl(int ty, int name);
void gen_stmt(int id);
int gen_generic_base_equal(int formal, int actual);
void gen_unify_formal(int formal, int actual);
void gen_bind_decl(int decl, int inst);
void gen_struct_decl_specialized(int decl, int inst, int cname);
void gen_function_params(int params, int is_extern);
void gen_function_type_params(int types);
void gen_function_signature_parts(int ret_ty, int name, int params, int is_extern);
void gen_function_specialized(int decl, int inst, int cname);
int gen_match_temp_symbol(void);
void gen_match_binding(int temp, int variant, int binding, int field);
void gen_match_stmt(int id);
void gen_tuple_decl(int ty, int name);
void gen_struct_decl(int id);
void gen_emit_complete_struct(int decl);
void gen_emit_complete_spec(int index);
void gen_emit_complete_type(int ty);
void gen_tagged_enum_decl(int id);
void gen_enum_decl(int id);
void gen_extern_param(int ty, int name);
void gen_function_signature(int id);
void gen_prototype(int id);
void gen_function(int id);
void gen_closure_emit_env(int serial, int closure_id);
void gen_closure_emit_value(int closure_id);
void gen_closure_emit_invoke_prototype(int serial, int closure_id);
void gen_closure_emit_call_helper(int sig);
void gen_closure_emit_factory(int serial, int closure_id);
void gen_closure_emit_invoke(int serial, int closure_id);
void gen_closure_emit_all(void);
void gen_program(int id);
int build_regression_ast(void);
void generator_regression_main(void);
void input_reset(void);
void input_put(int kind, int value, int text, int pos);
int input_peek(void);
int input_payload(void);
int input_text_payload(void);
int input_take(int kind);
int ast_generic_param(int name);
int ast_generic_params(void);
int ast_append_array_dimension(int ty, int size);
void ensure_ast_consts(int need);
void ast_const_add(int name, int value);
int ast_const_find(int name);
int ast_const_add_checked(int left, int right);
int ast_const_sub_checked(int left, int right);
int ast_const_mul_checked(int left, int right);
int ast_const_eval(int id);
int ast_array_dimension_value(void);
int ast_type(void);
int ast_call_args(void);
int ast_primary(void);
int ast_unary(void);
int ast_precedence(int kind);
int ast_operator(int kind);
int ast_compound_operator(int kind);
int ast_take_compound_operator(void);
int ast_compound_assign(int left, int op, int right);
int ast_expr_prec(int min_prec);
int ast_expr(void);
int clone_for_step(int step);
int lower_for_stmt(int id, int step);
int ast_alignment(void);
int ast_stmt(void);
int ast_params(void);
int ast_struct_decl(void);
int ast_enum_decl(void);
int ast_namespace_decl(void);
int ast_decl(void);
int ast_flatten_decl_list(int item);
int ast_program(void);
void c_source_reset(void);
void ensure_c_source(int need);
void c_source_put(int c);
void ensure_source_file_names(int need);
void ensure_source_file_text(int need);
int source_path_length(char *path);
int source_file_intern(char *path);
void ensure_source_deps(int need);
void source_dep_add(int from_id, int to_id);
int source_dep_reaches(int current, int target);
void source_import_fail(int kind, int file_id, int target_id, int line);
void source_reset(void);
void source_put(int c);
int is_space(int c);
int is_digit(int c);
int hex_digit_value(int c);
int is_hex_digit(int c);
int is_binary_digit(int c);
int is_alpha(int c);
int is_alnum(int c);
int source_peek(void);
int source_take(void);
int span_hash(int start, int length);
int span_equal(int a, int b, int length);
int sym_lookup(int start, int length, int h);
int sym_tag_id(void);
int sym_qualified(int ns, int name);
int ast_decl_name(int name);
int ast_type_name(int name);
int ast_is_match_default(int name);
int sym_intern(int start, int length, int kind, int scope);
void ensure_bi(int need);
void bi_register(char *text, int tc_tag, int flags);
int bi_lookup(int name);
int bi_tag(int name);
int bi_has_flag(int name, int flag);
void bi_init(void);
int word_code(int start, int length);
void ensure_lexer_literal_digits(int need);
int lexer_make_integer(int start, int length, int base);
void lexer_skip(void);
int lexer_next(void);
void include_process_line(int *line, int length);
void include_expand_handle(int *handle);
void load_source_file(char *path);
int map_token(int k);
void load_tokens_from_file(char *path);
void ensure_tc_vars(int need);
void ensure_tc_fun_meta(int need);
void ensure_tc_flow_frames(int need);
void ensure_tc_flow_log(int need);
void tc_flow_append_current(int count);
void tc_flow_append_fields(void);
int tc_flow_field_log_find(int start, int count, int root, int name);
void tc_flow_restore_fields(int frame);
void tc_flow_save_base(void);
void tc_flow_save_yes(void);
void tc_flow_restore_base(void);
void tc_flow_merge_yes(void);
void tc_flow_end(void);
void ensure_tc_scopes(int need);
void tc_enter_scope(void);
void tc_leave_scope(void);
void ensure_tc_path(int need);
void tc_fail(int code);
void tc_fail_types(int code, int expected_kind, int found_kind);
void ensure_tc_bindings(int need);
void tc_bind_clear(void);
void tc_bind_push(void);
void tc_bind_pop(void);
int tc_bind_find(int name);
int tc_bind_add(int name, int ty);
int tc_bind_explicit_args(int fun_node, int args);
int tc_is_integer_kind(int kind);
int tc_is_numeric_kind(int kind);
int tc_is_fixed_integer_kind(int kind);
int tc_is_legacy_integer_kind(int kind);
int tc_decimal_le(int raw, char *limit);
int tc_literal_fits(int id, int target_kind);
int tc_negative_integer_literal_conversion(int id, int target_kind, int actual_kind);
int tc_integer_literal_conversion(int id, int target_kind, int actual_kind);
int tc_type_is_structural(int kind);
int tc_type_equal(int a, int b);
int tc_signature_type(int entry);
int tc_type_node_from_summary(int kind, int name, int elem_kind, int elem_name);
int tc_generic_moves_array(int fun_node);
void tc_mark_float_expr(int id, int expected_kind);
void tc_match_generic_call_arg(int formal, int actual, int expr);
void tc_match_generic(int formal, int actual);
int tc_substitute_type(int ty);
int tc_same(int a_kind, int a_name, int b_kind, int b_name);
int tc_array_elem_same(int a_kind, int a_name, int b_kind, int b_name);
int tc_param_type_same(int a_name, int b_name);
int tc_same_full(int a_kind, int a_name, int a_elem_kind, int a_elem_name, int b_kind, int b_name,
                 int b_elem_kind, int b_elem_name);
int tc_ptr_diff_ok(int a_kind, int a_elem_kind, int a_elem_name, int b_kind, int b_elem_kind,
                   int b_elem_name);
int tc_find_struct(int name);
int tc_find_struct_ctx(int name, int ns);
int tc_find_enum(int name);
int tc_find_enum_ctx(int name, int ns);
int tc_generic_arity(int decl);
int tc_generic_arg_count(int ty);
int tc_named_exists_ctx(int name, int ns);
int tc_named_exists(int name);
void tc_check_type(int ty);
int tc_cycle_struct(int name);
int tc_cycle_type(int ty);
int tc_release_name(int name);
int tc_owned_initializer(int id);
int tc_is_owner_kind(int kind);
int tc_is_owner_type(int ty);
int tc_is_place(int id);
int tc_place_is_const(int id);
int tc_place_root(int id);
int tc_place_borrow_param(int id);
int tc_nth_arg(int head, int wanted);
int tc_provenance(int id);
int tc_provenance_origin(int id);
int tc_is_loan_ancestor(int ancestor, int origin);
int tc_borrow_conflict_from(int root, int origin, int requested_mut);
int tc_provenance_param(int id);
int tc_provenance_mut(int id);
void ensure_tc_field_targets(int need);
int tc_field_target_get(int root, int name);
void tc_field_target_set(int root, int name, int target);
int tc_field_target_root_for_place(int id);
int tc_fun_type_has_sensitive_param(int ty);
int tc_contract_param_position(int fun, int name);
void tc_contract_record(int fun, int candidate, int mutable);
int tc_contract_expr_param(int fun, int id);
int tc_contract_expr_mut(int fun, int id);
void tc_contract_scan_expr(int fun, int id);
void tc_contract_scan_stmt(int fun, int id);
void tc_prepare_return_contracts(int root);
void tc_check_mutable_place(int id);
int tc_borrow_conflict(int index);
int tc_mut_borrow_conflict(int index);
void tc_move_var(int index);
void tc_move_value(int id);
void tc_check_call_borrow(int arg, int mode);
void tc_check_return_escape(int source_index);
void tc_check_explicit_return_address_escape(int source_index);
void tc_record_borrow_ex(int destination, int source_index2, int mode, int origin);
void tc_record_borrow(int destination, int source_index2);
void tc_record_borrow_mut(int destination, int source_index2);
void tc_release_borrow(int index);
void tc_require_mutable(int id);
void tc_consume_call(int id);
void tc_add_var(int name, int kind, int named, int elem_kind, int elem_name, int type_node);
int tc_lookup_var(int name);
void tc_type_node(int ty);
int tc_numeric_result_kind(int a, int b);
int tc_integer_result_kind(int a, int b);
int tc_check_variant(int id);
void tc_check_closure_escape(int id);
void tc_check_closure_value_escape(int id);
void tc_attach_closure_caps(int destination, int caps);
void tc_expr(int id);
int tc_find_function(int name);
int sym_find_last_scope(int name);
int sym_prefix_scope(int name, int length);
int tc_find_function_ctx(int name, int ns);
int tc_find_enum_value(int name);
int tc_find_enum_variant(int name);
int tc_match_enum_decl(int ty);
int tc_match_variant_member(int decl, int name);
int tc_match_seen_variant(int head, int member);
void tc_match_check_arm_bindings(int variant, int bindings);
int tc_emit_field_type(int id);
int tc_emit_arg_type(int id);
int tc_expr_kind_for_emit(int id);
void tc_stmt(int id, int expected_kind, int expected_name);
int tc_diag_line(int pos);
int tc_diag_col(int pos);
int tc_diag_file(int pos);
void tc_print_source_byte(int value);
void tc_print_source_file(int file_id);
void tc_print_source_excerpt(int pos);
void tc_print_type_kind(int kind);
int tc_diag_has_types(int code);
void tc_print_hint(int code);
void tc_diag(void);
void source_import_diag(void);
int tc_check_function_symbols(int root);
int tc_reserved_function(int name);
int tc_ffi_struct_safe(int name);
int tc_ffi_enum_safe(int name);
int tc_check_ffi_type(int ty);
int tc_ffi_pointer_compatible(int ty);
int tc_check_ffi_return_type(int ty);
int tc_program(int root);
int pipeline_main(char *path);
void emit_symbol(int *out, int id);
void emit_string(int *out, int id);
void emit_identifier(int *out, int id);
void emit_print_prefix(int *out);
void emit_int_text(int *out, int value);
void emit_source_filename(int *out, int file_id);
void emit_source_line(int *out, int pos);
void emit_c_token(int *out, int kind, int value);
void emit_runtime(int *out);
void emit_c_file(char *path);
int basalt_sys_run(const char *executable, char **args, int arg_count, int max_output);
int basalt_sys_run_status(void);
char *basalt_sys_stdout(void);
char *basalt_sys_stderr(void);
int basalt_sys_truncated(void);
int basalt_sys_spawn_error(void);
int basalt_compile_argv(const char *compiler, const char *input_c, const char *output_bin,
                        char **all_args, int start, int extra_count);
int cli_arg_eq(char *a, char *b);
int main(int argc, char **argv);
int N_NONE = 0;
int N_INT = 1;
int N_BOOL = 2;
int N_STRING = 3;
int N_VAR = 4;
int N_BINOP = 5;
int N_CALL = 6;
int N_INDIRECT_CALL = 37;
int N_COMPOUND_ASSIGN = 38;
int N_DEREF = 7;
int N_INDEX = 8;
int N_ADDRESS = 9;
int N_LET = 10;
int N_ASSIGN = 11;
int N_PRINT = 12;
int N_IF = 13;
int N_WHILE = 14;
int N_BLOCK = 15;
int N_RETURN = 16;
int N_GLOBAL = 17;
int N_PARAM = 18;
int N_FUNC = 19;
int N_PROGRAM = 20;
int N_LIST = 21;
int N_EXPR = 22;
int N_BREAK = 23;
int N_CONTINUE = 24;
int N_FOR = 25;
int N_STRUCT = 26;
int N_ENUM = 27;
int N_FIELD = 28;
int N_FIELD_ACCESS = 29;
int N_CHAR = 30;
int N_NULL = 31;
int N_CONST = 32;
int N_FLOAT = 33;
int N_EXTERN = 34;
int N_GENERIC_STRUCT = 35;
int N_GENERIC_FUNC = 36;
int N_UNARY = 45;
int N_VARIANT = 39;
int N_DEFER = 40;
int N_MATCH = 41;
int N_MATCH_ARM = 42;
int N_TUPLE = 43;
int N_TUPLE_BIND = 44;
int N_MOVE = 46;
int N_CLOSURE = 47;
int N_CLOSURE_CALL = 48;
int N_PRINTLN = 49;
int OP_ADD = 1;
int OP_SUB = 2;
int OP_MUL = 3;
int OP_DIV = 4;
int OP_EQ = 5;
int OP_NEQ = 6;
int OP_LT = 7;
int OP_GT = 8;
int OP_AND = 9;
int OP_OR = 10;
int OP_CONCAT = 11;
int OP_BITAND = 12;
int OP_BITOR = 13;
int OP_BITXOR = 14;
int OP_SHL = 15;
int OP_SHR = 16;
int OP_MOD = 17;
int OP_NOT = 29;
int OP_LE = 30;
int OP_GE = 31;
int TY_INT = 1;
int TY_BOOL = 2;
int TY_STRING = 3;
int TY_VOID = 4;
int TY_PTR = 5;
int TY_ARRAY = 6;
int TY_NAMED = 7;
int TY_CHAR = 8;
int TY_FLOAT = 9;
int TY_DOUBLE = 10;
int TY_FUN = 11;
int TY_DYN_ARRAY = 13;
int TY_PARAM = 14;
int TY_GENERIC = 15;
int TY_LONG = 16;
int TY_LLONG = 17;
int TY_VARIANT = 18;
int TY_U8 = 19;
int TY_U16 = 20;
int TY_U32 = 21;
int TY_U64 = 22;
int TY_I8 = 23;
int TY_I16 = 24;
int TY_I32 = 25;
int TY_I64 = 26;
int TY_USIZE = 27;
int TY_TUPLE = 28;
int TY_CLOSURE = 29;
int *node_kind = 0;
int *node_a = 0;
int *node_b = 0;
int *node_c = 0;
int *node_next = 0;
int *node_value = 0;
int *node_aux = 0;
int *node_pos = 0;
int *node_scope = 0;
int *node_type = 0;
int ast_parse_mode = 0;
int node_count = 1;
int ast_namespace_scope = 0;
int *ast_const_name = 0;
int *ast_const_value = 0;
int ast_const_count = 0;
int ast_const_cap = 0;
int sym_tag_name = 0;
int node_cap = 0;
int payload_cap = 0;
int current_source_pos = 0;
int code_cap = 0;
int pipeline_root = 0;
int input_cap = 0;
int source_cap = 0;
int c_source_cap = 0;
int sym_cap = 0;
int *source_file_at = 0;
int *source_line_at = 0;
int *source_file_name_start = 0;
int *source_file_name_len = 0;
int *source_file_name_text = 0;
int source_file_count = 1;
int source_file_cap = 0;
int source_file_name_text_len = 0;
int source_file_name_text_cap = 0;
int source_active_file = 0;
int source_active_line = 1;
int *source_dep_from = 0;
int *source_dep_to = 0;
int source_dep_cap = 0;
int source_dep_count = 0;
int source_import_error_kind = 0;
int source_import_error_file = 0;
int source_import_error_target = 0;
int source_import_error_line = 1;
char *tc_diag_ascii =
    " !\"#$%&'()*+,-./"
    "0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
int gen_source_pos = 0;
int gen_source_epoch = 0;
int emit_pending_space = 0;
int emit_line_directives = 1;
int *ffi_header_ids = 0;
int ffi_header_count = 0;
int ffi_header_cap = 0;
int *payload_int = 0;
int *payload_name = 0;
int *payload_string = 0;
int payload_count = 1;
int C_KW = 1;
int C_IDENT = 2;
int C_INT = 3;
int C_STRING = 4;
int C_OP = 5;
int C_PUNCT = 6;
int C_NEWLINE = 7;
int C_RAW = 8;
int C_RAW_U64 = 9;
int *code_kind = 0;
int *code_value = 0;
int *code_pos = 0;
int *code_epoch = 0;
int code_count = 0;
int emit_for_step = 0;
int *emit_defer_expr = 0;
int *emit_defer_scope_start = 0;
int *emit_scope_start = 0;
int emit_defer_count = 0;
int emit_defer_cap = 0;
int emit_scope_depth = 0;
int emit_scope_cap = 0;
int *emit_loop_base = 0;
int emit_loop_depth = 0;
int emit_loop_cap = 0;
int *snapshot_kind = 0;
int *snapshot_value = 0;
int snapshot_cap = 0;
int *gen_bind_name = 0;
int *gen_bind_type = 0;
int gen_bind_count = 0;
int gen_bind_cap = 0;
int gen_active_function = 0;
int gen_active_closure = 0;
int gen_match_serial = 0;
int *gen_tuple_type = 0;
int *gen_tuple_name = 0;
int gen_tuple_count = 0;
int gen_tuple_cap = 0;
int *gen_closure_node = 0;
int *gen_closure_serial = 0;
int *gen_closure_sig = 0;
int *gen_closure_value_type_name = 0;
int gen_closure_count = 0;
int gen_closure_cap = 0;
int gen_closure_serial_next = 0;
int gen_mangle_start = 0;
int gen_mangle_len = 0;
int *gen_spec_kind = 0;
int *gen_spec_decl = 0;
int *gen_spec_type = 0;
int *gen_spec_name = 0;
int gen_spec_count = 0;
int gen_spec_cap = 0;
int gen_name_override = 0;
int gen_debug = 0;
int *gen_fun_chain_type = 0;
int gen_fun_chain_count = 0;
int gen_fun_chain_cap = 0;
int *gen_struct_state = 0;
int gen_struct_state_cap = 0;
int *gen_spec_state = 0;
int gen_spec_state_cap = 0;
int T_EOF = 0;
int T_LET = 1;
int T_FUNC = 2;
int T_ID = 3;
int T_INT = 4;
int T_STRING = 5;
int T_TRUE = 6;
int T_FALSE = 7;
int T_RETURN = 8;
int T_WHILE = 9;
int T_FOR = 10;
int T_BREAK = 11;
int T_CONTINUE = 12;
int T_IF = 13;
int T_THEN = 14;
int T_ELSE = 15;
int T_PRINT = 16;
int T_TINT = 17;
int T_TBOOL = 18;
int T_TSTRING = 19;
int T_TVOID = 20;
int T_PLUS = 21;
int T_MINUS = 22;
int T_STAR = 23;
int T_DIVIDE = 24;
int T_CONCAT = 25;
int T_AND_AND = 26;
int T_OR_OR = 27;
int T_EQUAL = 28;
int T_EQEQ = 29;
int T_NEQ = 30;
int T_LT = 31;
int T_GT = 32;
int T_COLON = 33;
int T_LPAREN = 34;
int T_RPAREN = 35;
int T_LBRACE = 36;
int T_RBRACE = 37;
int T_SEMI = 38;
int T_COMMA = 39;
int T_AMP = 40;
int T_LBRACK = 41;
int T_RBRACK = 42;
int T_STRUCT = 43;
int T_ENUM = 44;
int T_DOT = 45;
int T_CHAR = 46;
int T_NULL = 47;
int T_CONST = 48;
int T_TCHAR = 49;
int T_FLOAT = 50;
int T_TDOUBLE = 51;
int T_BITOR = 52;
int T_BITXOR = 53;
int T_BITNOT = 54;
int T_SHL = 55;
int T_SHR = 56;
int T_FN = 57;
int T_EXTERN = 58;
int T_ARRAY = 59;
int T_NAMESPACE = 60;
int T_SCOPE = 61;
int T_MOD = 62;
int T_PLUS_EQ = 63;
int T_MINUS_EQ = 64;
int T_STAR_EQ = 65;
int T_DIV_EQ = 66;
int T_MOD_EQ = 67;
int T_AMP_EQ = 68;
int T_BITOR_EQ = 69;
int T_BITXOR_EQ = 70;
int T_SHL_EQ = 71;
int T_SHR_EQ = 72;
int T_TLONG = 73;
int T_ALIGNAS = 74;
int T_TU8 = 75;
int T_TU16 = 76;
int T_TU32 = 77;
int T_TU64 = 78;
int T_TI8 = 79;
int T_TI16 = 80;
int T_TI32 = 81;
int T_TI64 = 82;
int T_TUSIZE = 83;
int T_DEFER = 84;
int T_MATCH = 85;
int T_FATARROW = 86;
int T_LE = 87;
int T_GE = 88;
int T_NOT = 89;
int T_MOVE = 90;
int T_BORROW = 91;
int T_BORROW_MUT = 92;
int T_CLOSURE = 93;
int T_PRINTLN = 94;
int T_MUT = 95;
int *input_kind = 0;
int *input_value = 0;
int *input_text = 0;
int *input_source_pos = 0;
int input_count = 0;
int input_pos = 0;
int debug_tokens = 0;
int for_step_context = 0;
int ast_generic_scope = 0;
int L_EOF = 0;
int L_ID = 1;
int L_INT = 2;
int L_FUNC = 3;
int L_LET = 4;
int L_PRINT = 5;
int L_PRINTLN = 96;
int L_RETURN = 6;
int L_IF = 7;
int L_ELSE = 8;
int L_WHILE = 9;
int L_TRUE = 10;
int L_FALSE = 11;
int L_TINT = 12;
int L_TBOOL = 13;
int L_TSTRING = 14;
int L_STRING = 35;
int L_TVOID = 15;
int L_THEN = 37;
int L_AMP = 34;
int L_PLUS = 16;
int L_MINUS = 17;
int L_STAR = 18;
int L_DIV = 19;
int L_EQ = 20;
int L_EQEQ = 21;
int L_NEQ = 22;
int L_LT = 23;
int L_GT = 24;
int L_LPAREN = 25;
int L_RPAREN = 26;
int L_LBRACE = 27;
int L_RBRACE = 28;
int L_COLON = 29;
int L_SEMI = 30;
int L_COMMA = 31;
int L_LBRACK = 32;
int L_RBRACK = 33;
int L_FOR = 38;
int L_BREAK = 39;
int L_CONTINUE = 40;
int L_CONCAT = 41;
int L_AND = 42;
int L_OR = 43;
int L_STRUCT = 44;
int L_ENUM = 45;
int L_DOT = 46;
int L_CHAR = 47;
int L_NULL = 48;
int L_CONST = 49;
int L_TCHAR = 50;
int L_FLOAT = 51;
int L_TFLOAT = 52;
int L_TDOUBLE = 53;
int L_BITOR = 54;
int L_BITXOR = 55;
int L_BITNOT = 56;
int L_SHL = 57;
int L_SHR = 58;
int L_FN = 59;
int L_EXTERN = 60;
int L_ARRAY = 61;
int L_NAMESPACE = 62;
int L_SCOPE = 63;
int L_MOD = 64;
int L_TLONG = 65;
int L_ALIGNAS = 76;
int L_TU8 = 77;
int L_TU16 = 78;
int L_TU32 = 79;
int L_TU64 = 80;
int L_TI8 = 81;
int L_TI16 = 82;
int L_TI32 = 83;
int L_TI64 = 84;
int L_TUSIZE = 85;
int L_PLUS_EQ = 66;
int L_MINUS_EQ = 67;
int L_STAR_EQ = 68;
int L_DIV_EQ = 69;
int L_MOD_EQ = 70;
int L_AMP_EQ = 71;
int L_BITOR_EQ = 72;
int L_BITXOR_EQ = 73;
int L_SHL_EQ = 74;
int L_SHR_EQ = 75;
int L_DEFER = 86;
int L_MATCH = 87;
int L_FATARROW = 88;
int L_LE = 89;
int L_GE = 90;
int L_MOVE = 91;
int L_BORROW = 92;
int L_BORROW_MUT = 93;
int L_NOT = 94;
int L_CLOSURE = 95;
int L_MUT = 97;
int *source = 0;
int source_len = 0;
int source_pos = 0;
int *c_source = 0;
int c_source_len = 0;
int include_ok = 1;
int include_line_initial_cap = 256;
int *sym_start = 0;
int *sym_len = 0;
int *sym_hash = 0;
int *sym_kind = 0;
int *sym_type = 0;
int *sym_elem_kind = 0;
int *sym_elem_name = 0;
int *sym_scope = 0;
int sym_count = 1;
int sym_text_len = 0;
int BI_TC_NONE = 0;
int BI_TC_VOID = 1;
int BI_TC_INT = 2;
int BI_TC_STRING = 3;
int BI_TC_PTR_INT = 4;
int BI_TC_PTR_VOID = 5;
int BI_TC_MEM_ALLOC = 6;
int BI_TC_MEM_ALLOC_ALIGNED = 30;
int BI_TC_SYS_RUN = 31;
int BI_TC_SYS_COMPILE = 34;
int BI_TC_SYS_STRING = 32;
int BI_TC_SYS_INT = 33;
int BI_TC_MEM_RESIZE = 7;
int BI_TC_MEM_FREE = 8;
int BI_TC_READ_LINE = 9;
int BI_TC_READ_INT = 10;
int BI_TC_WRITE_STRING = 11;
int BI_TC_WRITE_LINE = 12;
int BI_TC_WRITE_INT = 13;
int BI_TC_WRITE_CHAR = 14;
int BI_TC_IO_STATUS = 15;
int BI_TC_ATOMIC_MAKE = 16;
int BI_TC_ATOMIC_LOAD = 17;
int BI_TC_ATOMIC_STORE = 18;
int BI_TC_ATOMIC_FETCH_ADD = 19;
int BI_TC_ATOMIC_CAS = 20;
int BI_TC_ATOMIC_FREE = 21;
int BI_TC_CHANNEL_MAKE = 22;
int BI_TC_CHANNEL_SEND = 23;
int BI_TC_CHANNEL_RECV = 24;
int BI_TC_CHANNEL_CLOSE = 25;
int BI_TC_CHANNEL_FREE = 26;
int BI_TC_THREAD_SPAWN = 27;
int BI_TC_THREAD_JOIN = 28;
int BI_TC_THREAD_YIELD = 29;
int BI_FLAG_RESERVED = 1;
int BI_FLAG_OWNED = 2;
int BI_FLAG_CONSUME = 4;
int BI_FLAG_DYNFIELD = 16;
int BI_FLAG_MAIN = 32;
int bi_count = 0;
int bi_cap = 0;
int *bi_name = 0;
int *bi_len = 0;
int *bi_tc = 0;
int *bi_flags = 0;
int tok_kind = 0;
int tok_value = 0;
int tok_text = 0;
int tok_start = 0;
int tok_length = 0;
int lexer_error = 0;
int *lexer_literal_digits = 0;
int lexer_literal_digits_cap = 0;
int tc_root = 0;
int tc_ok = 1;
int tc_error_code = 0;
int tc_error_symbol = 0;
int tc_error_pos = (0 - 1);
int tc_error_expected_kind = 0;
int tc_error_found_kind = 0;
int tc_name = 0;
int tc_kind = 0;
int tc_elem_kind = 0;
int tc_elem_name = 0;
int tc_expected_elem_kind = 0;
int tc_expected_elem_name = 0;
int *tc_var_name = 0;
int *tc_var_kind = 0;
int *tc_var_named = 0;
int *tc_var_elem_kind = 0;
int *tc_var_elem_name = 0;
int *tc_var_type = 0;
int *tc_var_fun_target = 0;
int *tc_var_owned = 0;
int *tc_var_moved = 0;
int *tc_var_borrow_count = 0;
int *tc_var_borrow_mut = 0;
int *tc_var_borrow_source = 0;
int *tc_var_borrow_mode = 0;
int *tc_var_borrow_parent = 0;
int *tc_var_borrow_param = 0;
int *tc_var_param = 0;
int *tc_var_param_pos = 0;
int *tc_var_mode = 0;
int *tc_var_const = 0;
int *tc_var_closure_caps = 0;
int *tc_var_closure_moved = 0;
int *tc_var_ffi_borrowed = 0;
int tc_last_var_type = 0;
int tc_last_var_fun_target = 0;
int tc_last_var_owned = 0;
int tc_last_var_ffi_borrowed = 0;
int tc_expr_ffi_borrowed = 0;
int tc_last_var_moved = 0;
int tc_last_var_index = 0;
int tc_expr_borrow_source = (0 - 1);
int tc_expr_borrow_origin = (0 - 1);
int tc_expr_borrow_mut = 0;
int tc_expr_borrow_param = 0;
int tc_expr_owner_source = (0 - 1);
int tc_expr_is_owned = 0;
int tc_expr_fun_target = 0;
int tc_var_count = 0;
int tc_global_count = 0;
int tc_var_cap = 0;
int *tc_scope_start = 0;
int tc_scope_count = 0;
int tc_scope_cap = 0;
int *tc_path_name = 0;
int tc_path_count = 0;
int tc_path_cap = 0;
int tc_loop_depth = 0;
int tc_result_type = 0;
int tc_variant_enum = 0;
int tc_variant_member = 0;
int *tc_bind_name = 0;
int *tc_bind_type = 0;
int tc_bind_count = 0;
int tc_bind_cap = 0;
int *tc_bind_stack_name = 0;
int *tc_bind_stack_type = 0;
int tc_bind_stack_count = 0;
int tc_bind_stack_cap = 0;
int *tc_bind_frame_base = 0;
int *tc_bind_frame_size = 0;
int tc_bind_frame_depth = 0;
int tc_bind_frame_cap = 0;
int *tc_fun_return_param = 0;
int *tc_fun_return_mut = 0;
int tc_fun_meta_cap = 0;
int *tc_field_target_root = 0;
int *tc_field_target_name = 0;
int *tc_field_target_fun = 0;
int tc_field_target_count = 0;
int tc_field_target_cap = 0;
int *tc_flow_log_owned = 0;
int *tc_flow_log_moved = 0;
int *tc_flow_log_borrow_count = 0;
int *tc_flow_log_borrow_mut = 0;
int *tc_flow_log_source = 0;
int *tc_flow_log_mode = 0;
int *tc_flow_log_parent = 0;
int *tc_flow_log_borrow_param = 0;
int *tc_flow_log_ffi = 0;
int *tc_flow_log_closure_moved = 0;
int *tc_flow_log_fun_target = 0;
int *tc_flow_log_field_root = 0;
int *tc_flow_log_field_name = 0;
int *tc_flow_log_field_fun = 0;
int tc_flow_field_log_count = 0;
int *tc_flow_frame_field_base = 0;
int *tc_flow_frame_field_yes = 0;
int *tc_flow_frame_field_count = 0;
int *tc_flow_frame_field_yes_count = 0;
int *tc_flow_frame_base = 0;
int *tc_flow_frame_yes = 0;
int *tc_flow_frame_count = 0;
int *tc_flow_frame_has_yes = 0;
int tc_flow_log_count = 0;
int tc_flow_log_cap = 0;
int tc_flow_depth = 0;
int tc_flow_frame_cap = 0;
int tc_current_function = 0;
int tc_current_return_param = 0;
int tc_current_return_mut = 0;
int tc_current_return_seen = 0;
int tc_allow_outer_shadow = 0;
int next_capacity(int old, int need) {
  int n = old;
  if (n < 16)
    n = 16;
  else {
  }
  while (n < (need + 1)) {
    n = (n * 2);
  }
  return n;
}
void ensure_node(int need) {
  if (need < node_cap)
    return;
  else {
  }
  int n = next_capacity(node_cap, need);
  node_kind = grow_ints(node_kind, node_cap, n);
  node_a = grow_ints(node_a, node_cap, n);
  node_b = grow_ints(node_b, node_cap, n);
  node_c = grow_ints(node_c, node_cap, n);
  node_next = grow_ints(node_next, node_cap, n);
  node_value = grow_ints(node_value, node_cap, n);
  node_aux = grow_ints(node_aux, node_cap, n);
  node_pos = grow_ints(node_pos, node_cap, n);
  node_scope = grow_ints(node_scope, node_cap, n);
  node_type = grow_ints(node_type, node_cap, n);
  node_cap = n;
}
void ensure_payload(int need) {
  if (need < payload_cap)
    return;
  else {
  }
  int n = next_capacity(payload_cap, need);
  payload_int = grow_ints(payload_int, payload_cap, n);
  payload_name = grow_ints(payload_name, payload_cap, n);
  payload_string = grow_ints(payload_string, payload_cap, n);
  payload_cap = n;
}
void ensure_code(int need) {
  if (need < code_cap)
    return;
  else {
  }
  int n = next_capacity(code_cap, need);
  code_kind = grow_ints(code_kind, code_cap, n);
  code_value = grow_ints(code_value, code_cap, n);
  code_pos = grow_ints(code_pos, code_cap, n);
  code_epoch = grow_ints(code_epoch, code_cap, n);
  code_cap = n;
}
void ensure_input(int need) {
  if (need < input_cap)
    return;
  else {
  }
  int n = next_capacity(input_cap, need);
  input_kind = grow_ints(input_kind, input_cap, n);
  input_value = grow_ints(input_value, input_cap, n);
  input_text = grow_ints(input_text, input_cap, n);
  input_source_pos = grow_ints(input_source_pos, input_cap, n);
  input_cap = n;
}
void ensure_source(int need) {
  if (need < source_cap)
    return;
  else {
  }
  int n = next_capacity(source_cap, need);
  source = grow_ints(source, source_cap, n);
  source_file_at = grow_ints(source_file_at, source_cap, n);
  source_line_at = grow_ints(source_line_at, source_cap, n);
  source_cap = n;
}
void ensure_sym(int need) {
  if (need < sym_cap)
    return;
  else {
  }
  int n = next_capacity(sym_cap, need);
  sym_start = grow_ints(sym_start, sym_cap, n);
  sym_len = grow_ints(sym_len, sym_cap, n);
  sym_hash = grow_ints(sym_hash, sym_cap, n);
  sym_kind = grow_ints(sym_kind, sym_cap, n);
  sym_type = grow_ints(sym_type, sym_cap, n);
  sym_elem_kind = grow_ints(sym_elem_kind, sym_cap, n);
  sym_elem_name = grow_ints(sym_elem_name, sym_cap, n);
  sym_scope = grow_ints(sym_scope, sym_cap, n);
  sym_cap = n;
}
void ensure_ffi_headers(int need) {
  if (need < ffi_header_cap)
    return;
  else {
  }
  int n = next_capacity(ffi_header_cap, need);
  ffi_header_ids = grow_ints(ffi_header_ids, ffi_header_cap, n);
  ffi_header_cap = n;
}
int ffi_header_char_ok(int c) {
  if (c > 47) {
    if (c < 58)
      return 1;
    else {
    }
  } else {
  }
  if (c > 64) {
    if (c < 91)
      return 1;
    else {
    }
  } else {
  }
  if (c > 96) {
    if (c < 123)
      return 1;
    else {
    }
  } else {
  }
  if (c == 46)
    return 1;
  else {
  }
  if (c == 47)
    return 1;
  else {
  }
  if (c == 95)
    return 1;
  else {
  }
  if (c == 45)
    return 1;
  else {
  }
  return 0;
}
int ffi_header_valid(int id) {
  if (id == 0)
    return 1;
  else {
  }
  if (sym_len[id] == 0)
    return 0;
  else {
  }
  int i = 0;
  while (i < sym_len[id]) {
    if (ffi_header_char_ok(source[(sym_start[id] + i)]) == 0)
      return 0;
    else {
    }
    i = (i + 1);
  }
  return 1;
}
int ffi_header_seen(int id) {
  int i = 0;
  while (i < ffi_header_count) {
    if (ffi_header_ids[i] == id)
      return 1;
    else {
    }
    i = (i + 1);
  }
  return 0;
}
void ffi_header_register(int id) {
  if (id == 0)
    return;
  else {
  }
  if (ffi_header_seen(id) == 1)
    return;
  else {
  }
  (void)(ensure_ffi_headers(ffi_header_count));
  ffi_header_ids[ffi_header_count] = id;
  ffi_header_count = (ffi_header_count + 1);
}
int ast_node(int kind, int a, int b, int c, int value, int aux) {
  int id = node_count;
  int parse_pos = current_source_pos;
  if (((ast_parse_mode == 1) && (input_pos > 0)) && (input_pos < (input_count + 1)))
    parse_pos = input_source_pos[(input_pos - 1)];
  else {
  }
  (void)(ensure_node(id));
  node_kind[id] = kind;
  node_a[id] = a;
  node_b[id] = b;
  node_c[id] = c;
  node_next[id] = 0;
  node_value[id] = value;
  node_aux[id] = aux;
  node_pos[id] = parse_pos;
  node_scope[id] = ast_namespace_scope;
  node_type[id] = 0;
  node_count = (node_count + 1);
  return id;
}
int ast_link(int head, int item) {
  if (head == 0)
    return item;
  else {
  }
  int p = head;
  while (p != 0) {
    if (p == item) {
      int copy = ast_node(node_kind[item], node_a[item], node_b[item], node_c[item],
                          node_value[item], node_aux[item]);
      node_pos[copy] = node_pos[item];
      node_scope[copy] = node_scope[item];
      item = copy;
      { break; }
    } else {
    }
    p = node_next[p];
  }
  p = head;
  while (node_next[p] != 0) {
    p = node_next[p];
  }
  node_next[p] = item;
  return head;
}
int payload_make_int(int value) {
  int id = payload_count;
  (void)(ensure_payload(id));
  payload_int[id] = value;
  payload_count = (payload_count + 1);
  return id;
}
int payload_make_name(int name_id) {
  int id = payload_count;
  (void)(ensure_payload(id));
  payload_name[id] = name_id;
  payload_count = (payload_count + 1);
  return id;
}
int payload_make_string(int string_id) {
  int id = payload_count;
  (void)(ensure_payload(id));
  payload_string[id] = string_id;
  payload_count = (payload_count + 1);
  return id;
}
void ensure_snapshot(int need) {
  if (need < snapshot_cap)
    return;
  else {
  }
  int n = next_capacity(snapshot_cap, need);
  snapshot_kind = grow_ints(snapshot_kind, snapshot_cap, n);
  snapshot_value = grow_ints(snapshot_value, snapshot_cap, n);
  snapshot_cap = n;
}
void gen_bind_clear(void) {
  gen_bind_count = 0;
}
void ensure_gen_bind(int need) {
  if (need < gen_bind_cap)
    return;
  else {
  }
  int n = next_capacity(gen_bind_cap, need);
  gen_bind_name = grow_ints(gen_bind_name, gen_bind_cap, n);
  gen_bind_type = grow_ints(gen_bind_type, gen_bind_cap, n);
  gen_bind_cap = n;
}
int gen_bind_find(int name) {
  int i = 0;
  while (i < gen_bind_count) {
    if (gen_bind_name[i] == name)
      return gen_bind_type[i];
    else {
    }
    i = (i + 1);
  }
  return 0;
}
void gen_bind_add(int name, int ty) {
  int i = 0;
  while (i < gen_bind_count) {
    if (gen_bind_name[i] == name) {
      gen_bind_type[i] = ty;
      return;
    } else {
    }
    i = (i + 1);
  }
  (void)(ensure_gen_bind(gen_bind_count));
  gen_bind_name[gen_bind_count] = name;
  gen_bind_type[gen_bind_count] = ty;
  gen_bind_count = (gen_bind_count + 1);
}
void ensure_gen_tuple(int need) {
  if (need < gen_tuple_cap)
    return;
  else {
  }
  int n = next_capacity(gen_tuple_cap, need);
  gen_tuple_type = grow_ints(gen_tuple_type, gen_tuple_cap, n);
  gen_tuple_name = grow_ints(gen_tuple_name, gen_tuple_cap, n);
  gen_tuple_cap = n;
}
void ensure_gen_closure(int need) {
  if (need < gen_closure_cap)
    return;
  else {
  }
  int n = next_capacity(gen_closure_cap, need);
  gen_closure_node = grow_ints(gen_closure_node, gen_closure_cap, n);
  gen_closure_serial = grow_ints(gen_closure_serial, gen_closure_cap, n);
  gen_closure_sig = grow_ints(gen_closure_sig, gen_closure_cap, n);
  gen_closure_value_type_name = grow_ints(gen_closure_value_type_name, gen_closure_cap, n);
  gen_closure_cap = n;
}
int gen_closure_signature(int id) {
  int args = 0;
  int p = node_c[id];
  while (p != 0) {
    int src_ty = node_b[p];
    int copy_ty = ast_node(node_kind[src_ty], node_a[src_ty], node_b[src_ty], node_c[src_ty],
                           node_value[src_ty], node_aux[src_ty]);
    if (args == 0)
      args = copy_ty;
    else
      args = ast_link(args, copy_ty);
    p = node_next[p];
  }
  return ast_node(TY_CLOSURE, args, node_value[id], 0, 0, 0);
}
int gen_closure_name(int serial, char *prefix) {
  gen_mangle_start = (source_len + sym_text_len);
  gen_mangle_len = 0;
  (void)(gen_append_text(prefix));
  (void)(gen_append_uint(serial));
  return gen_mangle_intern(L_ID);
}
int gen_closure_env_name(int serial) {
  return gen_closure_name(serial, "__basalt_env_");
}
int gen_closure_invoke_name(int serial) {
  return gen_closure_name(serial, "__basalt_invoke_");
}
int gen_closure_factory_name(int serial) {
  return gen_closure_name(serial, "__basalt_make_");
}
int gen_closure_value_name(int serial) {
  return gen_closure_name(serial, "__basalt_closure_");
}
void gen_closure_register(int id) {
  if (id == 0)
    return;
  else {
  }
  int i = 0;
  while (i < gen_closure_count) {
    if (gen_closure_node[i] == id)
      return;
    else {
    }
    i = (i + 1);
  }
  (void)(ensure_gen_closure(gen_closure_count));
  int serial = gen_closure_serial_next;
  gen_closure_serial_next = (gen_closure_serial_next + 1);
  gen_closure_node[gen_closure_count] = id;
  gen_closure_serial[gen_closure_count] = serial;
  gen_closure_sig[gen_closure_count] = gen_closure_signature(id);
  gen_closure_value_type_name[gen_closure_count] =
      gen_mangled_type_symbol(gen_closure_sig[gen_closure_count]);
  node_aux[id] = (serial + 1);
  gen_closure_count = (gen_closure_count + 1);
}
void gen_append_char(int c) {
  int pos = ((source_len + sym_text_len) + gen_mangle_len);
  (void)(ensure_source(pos));
  source[pos] = c;
  gen_mangle_len = (gen_mangle_len + 1);
}
void gen_append_char_text(char c) {
  int pos = ((source_len + sym_text_len) + gen_mangle_len);
  (void)(ensure_source(pos));
  source[pos] = c;
  gen_mangle_len = (gen_mangle_len + 1);
}
void gen_append_text(char *text) {
  int i = 0;
  while (text[i] != 0) {
    (void)(gen_append_char_text(text[i]));
    i = (i + 1);
  }
}
void gen_append_symbol(int id) {
  int i = 0;
  while (i < sym_len[id]) {
    (void)(gen_append_char(source[(sym_start[id] + i)]));
    i = (i + 1);
  }
}
void gen_append_c_symbol(int id) {
  int i = 0;
  while (i < sym_len[id]) {
    int c = source[(sym_start[id] + i)];
    if (((c == 58) && ((i + 1) < sym_len[id])) && (source[((sym_start[id] + i) + 1)] == 58)) {
      (void)(gen_append_char(95));
      (void)(gen_append_char(95));
      i = (i + 2);
    } else {
      (void)(gen_append_char(c));
      i = (i + 1);
    }
  }
}
int gen_mangle_intern(int kind) {
  int h = span_hash(gen_mangle_start, gen_mangle_len);
  int old = sym_lookup(gen_mangle_start, gen_mangle_len, h);
  if (old != 0)
    return old;
  else {
  }
  int id = sym_intern(gen_mangle_start, gen_mangle_len, kind, 0);
  sym_text_len = (sym_text_len + gen_mangle_len);
  return id;
}
int sym_c_symbol(int id) {
  gen_mangle_start = (source_len + sym_text_len);
  gen_mangle_len = 0;
  (void)(gen_append_c_symbol(id));
  return gen_mangle_intern(L_ID);
}
void gen_append_uint(int value) {
  if (value > 9)
    (void)(gen_append_uint((value / 10)));
  else {
  }
  (void)(gen_append_char((48 + (value % 10))));
}
int gen_tuple_field_name(int index) {
  gen_mangle_start = (source_len + sym_text_len);
  gen_mangle_len = 0;
  (void)(gen_append_text("item"));
  (void)(gen_append_uint(index));
  return gen_mangle_intern(L_ID);
}
void gen_mangle_type(int ty) {
  if (ty == 0) {
    (void)(gen_append_text("void"));
    return;
  } else {
  }
  if (node_kind[ty] == TY_PARAM) {
    int b = gen_bind_find(node_value[ty]);
    if (b != 0) {
      (void)(gen_mangle_type(b));
      return;
    } else {
    }
    (void)(gen_append_c_symbol(node_value[ty]));
    return;
  } else {
  }
  if (node_kind[ty] == TY_INT)
    (void)(gen_append_text("int"));
  else if (node_kind[ty] == TY_BOOL)
    (void)(gen_append_text("bool"));
  else if (node_kind[ty] == TY_STRING)
    (void)(gen_append_text("char_ptr"));
  else if (node_kind[ty] == TY_CHAR)
    (void)(gen_append_text("char"));
  else if (node_kind[ty] == TY_FLOAT)
    (void)(gen_append_text("float"));
  else if (node_kind[ty] == TY_DOUBLE)
    (void)(gen_append_text("double"));
  else if (node_kind[ty] == TY_U8)
    (void)(gen_append_text("u8"));
  else if (node_kind[ty] == TY_U16)
    (void)(gen_append_text("u16"));
  else if (node_kind[ty] == TY_U32)
    (void)(gen_append_text("u32"));
  else if (node_kind[ty] == TY_U64)
    (void)(gen_append_text("u64"));
  else if (node_kind[ty] == TY_I8)
    (void)(gen_append_text("i8"));
  else if (node_kind[ty] == TY_I16)
    (void)(gen_append_text("i16"));
  else if (node_kind[ty] == TY_I32)
    (void)(gen_append_text("i32"));
  else if (node_kind[ty] == TY_I64)
    (void)(gen_append_text("i64"));
  else if (node_kind[ty] == TY_USIZE)
    (void)(gen_append_text("usize"));
  else if (node_kind[ty] == TY_VOID)
    (void)(gen_append_text("void"));
  else if (node_kind[ty] == TY_NAMED)
    (void)(gen_append_c_symbol(node_value[ty]));
  else if (node_kind[ty] == TY_PTR) {
    (void)(gen_append_text("ptr_"));
    (void)(gen_mangle_type(node_a[ty]));
  } else if ((node_kind[ty] == TY_ARRAY) || (node_kind[ty] == TY_DYN_ARRAY)) {
    (void)(gen_append_text("array_"));
    (void)(gen_mangle_type(node_a[ty]));
  } else if (node_kind[ty] == TY_GENERIC) {
    (void)(gen_append_c_symbol(node_value[ty]));
    (void)(gen_append_text("__"));
    int a = node_a[ty];
    while (a != 0) {
      (void)(gen_mangle_type(a));
      if (node_next[a] != 0)
        (void)(gen_append_text("__"));
      else {
      }
      a = node_next[a];
    }
  } else if (node_kind[ty] == TY_TUPLE) {
    (void)(gen_append_text("tuple"));
    int t = node_a[ty];
    while (t != 0) {
      (void)(gen_append_text("__"));
      (void)(gen_mangle_type(t));
      t = node_next[t];
    }
  } else if (node_kind[ty] == TY_CLOSURE) {
    (void)(gen_append_text("closure"));
    int a = node_a[ty];
    while (a != 0) {
      (void)(gen_append_text("__"));
      (void)(gen_mangle_type(a));
      a = node_next[a];
    }
    (void)(gen_append_text("__ret__"));
    (void)(gen_mangle_type(node_b[ty]));
  } else
    (void)(gen_append_text("int"));
}
int gen_mangled_type_symbol(int ty) {
  gen_mangle_start = (source_len + sym_text_len);
  gen_mangle_len = 0;
  (void)(gen_mangle_type(ty));
  return gen_mangle_intern(0);
}
void gen_add_tuple_type(int ty) {
  if ((ty == 0) || (node_kind[ty] != TY_TUPLE))
    return;
  else {
  }
  int name = gen_mangled_type_symbol(ty);
  int i = 0;
  while (i < gen_tuple_count) {
    if (gen_tuple_name[i] == name)
      return;
    else {
    }
    i = (i + 1);
  }
  int item = node_a[ty];
  while (item != 0) {
    (void)(gen_collect_type(item));
    item = node_next[item];
  }
  (void)(ensure_gen_tuple(gen_tuple_count));
  gen_tuple_type[gen_tuple_count] = ty;
  gen_tuple_name[gen_tuple_count] = name;
  gen_tuple_count = (gen_tuple_count + 1);
}
int gen_mangled_function_symbol(int base, int args) {
  gen_mangle_start = (source_len + sym_text_len);
  gen_mangle_len = 0;
  (void)(gen_append_c_symbol(base));
  (void)(gen_append_text("__"));
  int a = args;
  while (a != 0) {
    (void)(gen_mangle_type(a));
    if (node_next[a] != 0)
      (void)(gen_append_text("__"));
    else {
    }
    a = node_next[a];
  }
  return gen_mangle_intern(0);
}
void code_emit(int kind, int value) {
  (void)(ensure_code(code_count));
  code_kind[code_count] = kind;
  code_value[code_count] = value;
  code_pos[code_count] = gen_source_pos;
  code_epoch[code_count] = gen_source_epoch;
  code_count = (code_count + 1);
}
void code_reset(void) {
  code_count = 0;
}
void ensure_emit_defer(int need) {
  if (need < emit_defer_cap)
    return;
  else {
  }
  int n = next_capacity(emit_defer_cap, need);
  emit_defer_expr = grow_ints(emit_defer_expr, emit_defer_cap, n);
  emit_defer_scope_start = grow_ints(emit_defer_scope_start, emit_defer_cap, n);
  emit_defer_cap = n;
}
void ensure_emit_scope(int need) {
  if (need < emit_scope_cap)
    return;
  else {
  }
  int n = next_capacity(emit_scope_cap, need);
  emit_scope_start = grow_ints(emit_scope_start, emit_scope_cap, n);
  emit_scope_cap = n;
}
void ensure_emit_loop(int need) {
  if (need < emit_loop_cap)
    return;
  else {
  }
  int n = next_capacity(emit_loop_cap, need);
  emit_loop_base = grow_ints(emit_loop_base, emit_loop_cap, n);
  emit_loop_cap = n;
}
void gen_defer_push(int expr) {
  (void)(ensure_emit_defer(emit_defer_count));
  emit_defer_expr[emit_defer_count] = expr;
  emit_defer_count = (emit_defer_count + 1);
}
void gen_emit_defer_from(int base) {
  int i = (emit_defer_count - 1);
  while ((i + 1) > base) {
    (void)(gen_expr(emit_defer_expr[i]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
    i = (i - 1);
  }
}
void gen_emit_all_defers(void) {
  (void)(gen_emit_defer_from(0));
}
void ensure_gen_specs(int need) {
  if (need < gen_spec_cap)
    return;
  else {
  }
  int n = next_capacity(gen_spec_cap, need);
  gen_spec_kind = grow_ints(gen_spec_kind, gen_spec_cap, n);
  gen_spec_decl = grow_ints(gen_spec_decl, gen_spec_cap, n);
  gen_spec_type = grow_ints(gen_spec_type, gen_spec_cap, n);
  gen_spec_name = grow_ints(gen_spec_name, gen_spec_cap, n);
  gen_spec_cap = n;
}
void ensure_gen_struct_state(int need) {
  if (need < gen_struct_state_cap)
    return;
  else {
  }
  int n = next_capacity(gen_struct_state_cap, need);
  gen_struct_state = grow_ints(gen_struct_state, gen_struct_state_cap, n);
  gen_struct_state_cap = n;
}
void ensure_gen_spec_state(int need) {
  if (need < gen_spec_state_cap)
    return;
  else {
  }
  int n = next_capacity(gen_spec_state_cap, need);
  gen_spec_state = grow_ints(gen_spec_state, gen_spec_state_cap, n);
  gen_spec_state_cap = n;
}
int gen_find_spec_index(int decl, int name) {
  int i = 0;
  while (i < gen_spec_count) {
    if (gen_spec_kind[i] == 1) {
      if (gen_spec_decl[i] == decl) {
        if (gen_spec_name[i] == name)
          return i;
        else {
        }
      } else {
      }
    } else {
    }
    i = (i + 1);
  }
  return (0 - 1);
}
int gen_substitute_type(int ty) {
  if (ty == 0)
    return 0;
  else {
  }
  if (node_kind[ty] == TY_PARAM) {
    int b = gen_bind_find(node_value[ty]);
    if ((b != 0) && (b != ty))
      return gen_substitute_type(b);
    else {
    }
    return ast_node(TY_PARAM, node_a[ty], node_b[ty], node_c[ty], node_value[ty], node_aux[ty]);
  } else {
  }
  if (node_kind[ty] == TY_PTR)
    return ast_node(TY_PTR, gen_substitute_type(node_a[ty]), 0, 0, 0, 0);
  else {
  }
  if (node_kind[ty] == TY_ARRAY)
    return ast_node(TY_ARRAY, gen_substitute_type(node_a[ty]), 0, 0, node_value[ty], 0);
  else {
  }
  if (node_kind[ty] == TY_DYN_ARRAY)
    return ast_node(TY_DYN_ARRAY, gen_substitute_type(node_a[ty]), 0, 0, 0, 0);
  else {
  }
  if (node_kind[ty] == TY_GENERIC) {
    int generic_name = node_value[ty];
    int generic_decl = tc_find_struct_ctx(generic_name, node_scope[ty]);
    if (generic_decl != 0)
      generic_name = node_value[generic_decl];
    else {
    }
    int args = 0;
    int p = node_a[ty];
    while (p != 0) {
      int q = gen_substitute_type(p);
      if (args == 0)
        args = q;
      else
        args = ast_link(args, q);
      p = node_next[p];
    }
    int result_generic = ast_node(TY_GENERIC, args, 0, 0, generic_name, 0);
    node_scope[result_generic] = node_scope[ty];
    return result_generic;
  } else {
  }
  if ((node_kind[ty] == TY_FUN) || (node_kind[ty] == TY_CLOSURE)) {
    int args2 = 0;
    int p2 = node_a[ty];
    while (p2 != 0) {
      int q2 = gen_substitute_type(p2);
      if (args2 == 0)
        args2 = q2;
      else
        args2 = ast_link(args2, q2);
      p2 = node_next[p2];
    }
    return ast_node(node_kind[ty], args2, gen_substitute_type(node_b[ty]), 0, 0, 0);
  } else {
  }
  return ast_node(node_kind[ty], node_a[ty], node_b[ty], node_c[ty], node_value[ty], node_aux[ty]);
}
int gen_active_closure_capture(int name) {
  if (gen_active_closure == 0)
    return 0;
  else {
  }
  int cap_probe = node_a[gen_active_closure];
  while (cap_probe != 0) {
    if (node_a[cap_probe] == name)
      return cap_probe;
    else {
    }
    cap_probe = node_next[cap_probe];
  }
  return 0;
}
int gen_closure_env_param_name(int serial) {
  return gen_closure_name(serial, "__basalt_env_arg_");
}
int gen_closure_env_local_name(int serial) {
  return gen_closure_name(serial, "__basalt_env_ptr_");
}
int gen_closure_active_env_name(void) {
  if (gen_active_closure == 0)
    return gen_closure_env_local_name(0);
  else {
  }
  return gen_closure_env_local_name((node_aux[gen_active_closure] - 1));
}
int gen_closure_arg_name(int index) {
  gen_mangle_start = (source_len + sym_text_len);
  gen_mangle_len = 0;
  (void)(gen_append_text("__basalt_arg_"));
  (void)(gen_append_uint(index));
  return gen_mangle_intern(L_ID);
}
int gen_closure_call_name(int sig) {
  int sig_name = gen_mangled_type_symbol(sig);
  gen_mangle_start = (source_len + sym_text_len);
  gen_mangle_len = 0;
  (void)(gen_append_text("__basalt_call_"));
  (void)(gen_append_symbol(sig_name));
  return gen_mangle_intern(L_ID);
}
int gen_active_param_type(int name) {
  if (gen_active_function == 0)
    return 0;
  else {
  }
  int p = node_c[gen_active_function];
  while (p != 0) {
    if (node_a[p] == name)
      return node_b[p];
    else {
    }
    p = node_next[p];
  }
  return 0;
}
int gen_local_decl_type(int id, int name) {
  if (id == 0)
    return 0;
  else {
  }
  int k = node_kind[id];
  if ((k == N_LET) || (k == N_CONST)) {
    if (node_a[id] == name)
      return node_b[id];
    else {
    }
    return gen_local_decl_type(node_next[id], name);
  } else {
  }
  if (k == N_BLOCK) {
    int item = node_a[id];
    while (item != 0) {
      int found = gen_local_decl_type(item, name);
      if (found != 0)
        return found;
      else {
      }
      item = node_next[item];
    }
    return 0;
  } else {
  }
  if (k == N_IF) {
    int found_if = gen_local_decl_type(node_b[id], name);
    if (found_if != 0)
      return found_if;
    else {
    }
    return gen_local_decl_type(node_c[id], name);
  } else {
  }
  if (k == N_WHILE)
    return gen_local_decl_type(node_b[id], name);
  else {
  }
  if (k == N_FOR) {
    int found_for = gen_local_decl_type(node_a[id], name);
    if (found_for != 0)
      return found_for;
    else {
    }
    found_for = gen_local_decl_type(node_c[id], name);
    if (found_for != 0)
      return found_for;
    else {
    }
    return gen_local_decl_type(node_value[id], name);
  } else {
  }
  if (k == N_MATCH) {
    int arm = node_b[id];
    while (arm != 0) {
      int found_arm = gen_local_decl_type(node_b[arm], name);
      if (found_arm != 0)
        return found_arm;
      else {
      }
      arm = node_next[arm];
    }
  } else {
  }
  return gen_local_decl_type(node_next[id], name);
}
int gen_active_local_decl_type(int name) {
  if (gen_active_function == 0)
    return 0;
  else {
  }
  return gen_local_decl_type(node_a[gen_active_function], name);
}
int gen_spec_exists(int kind, int decl, int name) {
  int i = 0;
  while (i < gen_spec_count) {
    if (((gen_spec_kind[i] == kind) && (gen_spec_decl[i] == decl)) && (gen_spec_name[i] == name))
      return 1;
    else {
    }
    i = (i + 1);
  }
  return 0;
}
void gen_collect_struct_fields(int decl, int inst) {
  int saved_count = gen_bind_count;
  (void)(ensure_gen_bind((saved_count + saved_count)));
  int save_i = 0;
  while (save_i < saved_count) {
    gen_bind_name[(saved_count + save_i)] = gen_bind_name[save_i];
    gen_bind_type[(saved_count + save_i)] = gen_bind_type[save_i];
    save_i = (save_i + 1);
  }
  (void)(gen_bind_decl(decl, inst));
  int field = node_a[decl];
  while (field != 0) {
    int field_type = gen_substitute_type(node_b[field]);
    (void)(gen_collect_type(field_type));
    field = node_next[field];
  }
  (void)(gen_bind_clear());
  int restore_i = 0;
  while (restore_i < saved_count) {
    gen_bind_name[restore_i] = gen_bind_name[(saved_count + restore_i)];
    gen_bind_type[restore_i] = gen_bind_type[(saved_count + restore_i)];
    restore_i = (restore_i + 1);
  }
  gen_bind_count = saved_count;
}
void gen_add_struct_spec(int ty) {
  int q = gen_substitute_type(ty);
  if ((q == 0) || (node_kind[q] != TY_GENERIC))
    return;
  else {
  }
  int decl = tc_find_struct(node_value[q]);
  if (decl == 0)
    return;
  else {
  }
  int name = gen_mangled_type_symbol(q);
  if (gen_spec_exists(1, decl, name) == 1)
    return;
  else {
  }
  int slot = gen_spec_count;
  (void)(ensure_gen_specs(gen_spec_count));
  gen_spec_kind[slot] = 1;
  gen_spec_decl[slot] = decl;
  gen_spec_type[slot] = q;
  gen_spec_name[slot] = name;
  gen_spec_count = (gen_spec_count + 1);
  int a = node_a[q];
  while (a != 0) {
    (void)(gen_collect_type(a));
    a = node_next[a];
  }
  (void)(gen_collect_struct_fields(decl, q));
  int last = (gen_spec_count - 1);
  while (slot < last) {
    gen_spec_kind[slot] = gen_spec_kind[(slot + 1)];
    gen_spec_decl[slot] = gen_spec_decl[(slot + 1)];
    gen_spec_type[slot] = gen_spec_type[(slot + 1)];
    gen_spec_name[slot] = gen_spec_name[(slot + 1)];
    slot = (slot + 1);
  }
  gen_spec_kind[last] = 1;
  gen_spec_decl[last] = decl;
  gen_spec_type[last] = q;
  gen_spec_name[last] = name;
}
int gen_type_has_param(int ty) {
  if (ty == 0)
    return 0;
  else {
  }
  if (node_kind[ty] == TY_PARAM)
    return 1;
  else {
  }
  if (((node_kind[ty] == TY_PTR) || (node_kind[ty] == TY_ARRAY)) || (node_kind[ty] == TY_DYN_ARRAY))
    return gen_type_has_param(node_a[ty]);
  else {
  }
  if (node_kind[ty] == TY_GENERIC) {
    int a = node_a[ty];
    while (a != 0) {
      if (gen_type_has_param(a) == 1)
        return 1;
      else {
      }
      a = node_next[a];
    }
  } else {
  }
  return 0;
}
void gen_add_fun_spec(int decl, int args) {
  int saved_count = gen_bind_count;
  (void)(ensure_gen_bind((saved_count + saved_count)));
  int save_i = 0;
  while (save_i < saved_count) {
    gen_bind_name[(saved_count + save_i)] = gen_bind_name[save_i];
    gen_bind_type[(saved_count + save_i)] = gen_bind_type[save_i];
    save_i = (save_i + 1);
  }
  int actual = 0;
  int p = args;
  while (p != 0) {
    int q = gen_substitute_type(p);
    if (actual == 0)
      actual = q;
    else
      actual = ast_link(actual, q);
    p = node_next[p];
  }
  (void)(gen_bind_decl(decl, actual));
  int typeargs = 0;
  int tp = node_aux[decl];
  while (tp != 0) {
    int bt = gen_bind_find(node_a[tp]);
    if (bt == 0)
      bt = ast_node(TY_PARAM, 0, 0, 0, node_a[tp], 0);
    else {
    }
    int cq = gen_substitute_type(bt);
    if (typeargs == 0)
      typeargs = cq;
    else
      typeargs = ast_link(typeargs, cq);
    tp = node_next[tp];
  }
  (void)(gen_bind_clear());
  int restore_i = 0;
  while (restore_i < saved_count) {
    gen_bind_name[restore_i] = gen_bind_name[(saved_count + restore_i)];
    gen_bind_type[restore_i] = gen_bind_type[(saved_count + restore_i)];
    restore_i = (restore_i + 1);
  }
  gen_bind_count = saved_count;
  int name = gen_mangled_function_symbol(node_value[decl], typeargs);
  if (gen_spec_exists(2, decl, name) == 1)
    return;
  else {
  }
  (void)(ensure_gen_specs(gen_spec_count));
  gen_spec_kind[gen_spec_count] = 2;
  gen_spec_decl[gen_spec_count] = decl;
  gen_spec_type[gen_spec_count] = actual;
  gen_spec_name[gen_spec_count] = name;
  gen_spec_count = (gen_spec_count + 1);
  int a = actual;
  while (a != 0) {
    (void)(gen_collect_type(a));
    a = node_next[a];
  }
}
void gen_collect_type(int ty) {
  if (ty == 0) {
    return;
  } else {
  }
  if (node_kind[ty] == TY_GENERIC) {
    (void)(gen_add_struct_spec(ty));
  } else if (node_kind[ty] == TY_TUPLE) {
    (void)(gen_add_tuple_type(ty));
  } else if (node_kind[ty] == TY_CLOSURE) {
    int ca = node_a[ty];
    while (ca != 0) {
      (void)(gen_collect_type(ca));
      ca = node_next[ca];
    }
    (void)(gen_collect_type(node_b[ty]));
  } else if (((node_kind[ty] == TY_PTR) || (node_kind[ty] == TY_ARRAY)) ||
             (node_kind[ty] == TY_DYN_ARRAY)) {
    (void)(gen_collect_type(node_a[ty]));
  } else {
  }
}
void gen_collect_expr(int id) {
  if (id == 0)
    return;
  else {
  }
  int k = node_kind[id];
  if (k == N_CLOSURE) {
    int cp = node_c[id];
    while (cp != 0) {
      (void)(gen_collect_type(node_b[cp]));
      cp = node_next[cp];
    }
    (void)(gen_collect_type(node_value[id]));
    (void)(gen_closure_register(id));
    (void)(gen_collect_stmt(node_b[id]));
    return;
  } else {
  }
  if (k == N_VARIANT) {
    int aa = node_a[id];
    while (aa != 0) {
      (void)(gen_collect_expr(aa));
      aa = node_next[aa];
    }
    return;
  } else {
  }
  if (k == N_CALL) {
    int f = tc_find_function_ctx(node_value[id], node_scope[id]);
    if ((f != 0) && (node_kind[f] == N_GENERIC_FUNC)) {
      int aa = node_a[id];
      int actual = node_b[id];
      if (actual != 0) {
        int explicit = actual;
        while (explicit != 0) {
          (void)(gen_collect_type(explicit));
          explicit = node_next[explicit];
        }
      } else {
        while (aa != 0) {
          int q = tc_emit_arg_type(aa);
          if (q == 0) {
            (void)(tc_expr(aa));
            q = tc_result_type;
            if (q == 0)
              q = tc_type_node_from_summary(tc_kind, tc_name, tc_elem_kind, tc_elem_name);
            else {
            }
          } else {
          }
          if (q != 0)
            q = gen_substitute_type(q);
          else {
          }
          if (actual == 0)
            actual = q;
          else
            actual = ast_link(actual, q);
          (void)(gen_collect_type(q));
          aa = node_next[aa];
        }
      }
      int unresolved = 0;
      int check_actual = actual;
      while (check_actual != 0) {
        if (gen_type_has_param(check_actual) == 1)
          unresolved = 1;
        else {
        }
        check_actual = node_next[check_actual];
      }
      if (unresolved == 0)
        (void)(gen_add_fun_spec(f, actual));
      else {
      }
    } else {
    }
    int aar = node_a[id];
    while (aar != 0) {
      (void)(gen_collect_expr(aar));
      aar = node_next[aar];
    }
    return;
  } else {
  }
  if (k == N_INDIRECT_CALL) {
    (void)(gen_collect_expr(node_a[id]));
    int aa = node_b[id];
    while (aa != 0) {
      (void)(gen_collect_expr(aa));
      aa = node_next[aa];
    }
    return;
  } else {
  }
  if (k == N_MOVE) {
    (void)(gen_collect_expr(node_a[id]));
    return;
  } else {
  }
  if (k == N_BINOP) {
    (void)(gen_collect_expr(node_a[id]));
    (void)(gen_collect_expr(node_b[id]));
    return;
  } else {
  }
  if (k == N_TUPLE) {
    int item = node_a[id];
    while (item != 0) {
      (void)(gen_collect_expr(item));
      item = node_next[item];
    }
    return;
  } else {
  }
  if ((((k == N_FIELD_ACCESS) || (k == N_INDEX)) || (k == N_DEREF)) || (k == N_ADDRESS)) {
    (void)(gen_collect_expr(node_a[id]));
    (void)(gen_collect_expr(node_b[id]));
    return;
  } else {
  }
}
void gen_collect_stmt(int id) {
  if (id == 0)
    return;
  else {
  }
  int k = node_kind[id];
  if (k == N_DEFER) {
    (void)(gen_collect_expr(node_a[id]));
    return;
  } else {
  }
  if (k == N_TUPLE_BIND) {
    (void)(gen_collect_type(node_b[id]));
    (void)(gen_collect_expr(node_c[id]));
    return;
  } else {
  }
  if (k == N_TUPLE) {
    (void)(gen_collect_expr(node_a[id]));
    return;
  } else {
  }
  if (k == N_MATCH) {
    (void)(gen_collect_expr(node_a[id]));
    int ma = node_b[id];
    while (ma != 0) {
      (void)(gen_collect_stmt(node_b[ma]));
      ma = node_next[ma];
    }
    return;
  } else {
  }
  if ((k == N_LET) || (k == N_CONST)) {
    (void)(gen_collect_type(node_b[id]));
    (void)(gen_collect_expr(node_c[id]));
    return;
  } else {
  }
  if (k == N_GLOBAL) {
    (void)(gen_collect_type(node_b[id]));
    (void)(gen_collect_expr(node_c[id]));
    return;
  } else {
  }
  if ((k == N_ASSIGN) || (k == N_COMPOUND_ASSIGN)) {
    (void)(gen_collect_expr(node_a[id]));
    (void)(gen_collect_expr(node_b[id]));
    return;
  } else {
  }
  if ((((k == N_PRINT) || (k == N_PRINTLN)) || (k == N_EXPR)) || (k == N_RETURN)) {
    (void)(gen_collect_expr(node_a[id]));
    return;
  } else {
  }
  if (k == N_BLOCK) {
    int x = node_a[id];
    while (x != 0) {
      (void)(gen_collect_stmt(x));
      x = node_next[x];
    }
    return;
  } else {
  }
  if (k == N_IF) {
    (void)(gen_collect_expr(node_a[id]));
    (void)(gen_collect_stmt(node_b[id]));
    (void)(gen_collect_stmt(node_c[id]));
    return;
  } else {
  }
  if (k == N_WHILE) {
    (void)(gen_collect_expr(node_a[id]));
    (void)(gen_collect_stmt(node_b[id]));
    return;
  } else {
  }
  if (k == N_FOR) {
    (void)(gen_collect_stmt(node_a[id]));
    (void)(gen_collect_expr(node_b[id]));
    (void)(gen_collect_stmt(node_c[id]));
    (void)(gen_collect_stmt(node_value[id]));
    return;
  } else {
  }
}
void gen_alignment(int alignment) {
  if (alignment > 0) {
    (void)(code_emit(C_IDENT, (0 - 1020)));
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_INT, alignment));
    (void)(code_emit(C_PUNCT, 5));
  } else {
  }
}
void gen_primitive_type(int kind) {
  if (kind == TY_INT)
    (void)(code_emit(C_KW, 1));
  else if (kind == TY_BOOL)
    (void)(code_emit(C_KW, 2));
  else if (kind == TY_STRING)
    (void)(code_emit(C_KW, 3));
  else if (kind == TY_CHAR)
    (void)(code_emit(C_KW, 17));
  else if (kind == TY_FLOAT)
    (void)(code_emit(C_KW, 18));
  else if (kind == TY_DOUBLE)
    (void)(code_emit(C_KW, 15));
  else if (kind == TY_LONG)
    (void)(code_emit(C_KW, 19));
  else if (kind == TY_LLONG)
    (void)(code_emit(C_KW, 20));
  else if (kind == TY_U8)
    (void)(code_emit(C_KW, 21));
  else if (kind == TY_U16)
    (void)(code_emit(C_KW, 22));
  else if (kind == TY_U32)
    (void)(code_emit(C_KW, 23));
  else if (kind == TY_U64)
    (void)(code_emit(C_KW, 24));
  else if (kind == TY_I8)
    (void)(code_emit(C_KW, 25));
  else if (kind == TY_I16)
    (void)(code_emit(C_KW, 26));
  else if (kind == TY_I32)
    (void)(code_emit(C_KW, 27));
  else if (kind == TY_I64)
    (void)(code_emit(C_KW, 28));
  else if (kind == TY_USIZE)
    (void)(code_emit(C_KW, 29));
  else if (kind == TY_VOID)
    (void)(code_emit(C_KW, 4));
  else {
  }
}
void gen_type(int kind, int child, int size) {
  if (kind == TY_PTR) {
    (void)(gen_type(node_kind[node_a[child]], node_a[child], 0));
    (void)(code_emit(C_PUNCT, 1));
  } else if (kind == TY_ARRAY) {
    (void)(gen_type(node_kind[child], node_a[child], node_value[child]));
    (void)(code_emit(C_PUNCT, 2));
    (void)(code_emit(C_INT, size));
    (void)(code_emit(C_PUNCT, 3));
  } else if (kind == TY_DYN_ARRAY) {
    (void)(code_emit(C_IDENT, (0 - 1003)));
    (void)(code_emit(C_PUNCT, 18));
  } else if (kind == TY_NAMED) {
    (void)(code_emit(C_IDENT, sym_c_symbol(node_value[child])));
    (void)(code_emit(C_PUNCT, 18));
  } else if (kind == TY_GENERIC) {
    (void)(code_emit(C_IDENT, gen_mangled_type_symbol(child)));
    (void)(code_emit(C_PUNCT, 18));
  } else if (kind == TY_TUPLE) {
    (void)(code_emit(C_KW, 12));
    (void)(code_emit(C_IDENT, gen_mangled_type_symbol(child)));
    (void)(code_emit(C_PUNCT, 18));
  } else if (kind == TY_CLOSURE) {
    (void)(code_emit(C_KW, 12));
    (void)(code_emit(C_IDENT, gen_mangled_type_symbol(child)));
    (void)(code_emit(C_PUNCT, 18));
  } else if (kind == TY_PARAM) {
    int b = gen_bind_find(node_value[child]);
    if ((b != 0) && (b != child))
      (void)(gen_type(node_kind[b], b, node_value[b]));
    else
      (void)(code_emit(C_IDENT, sym_c_symbol(node_value[child])));
    (void)(code_emit(C_PUNCT, 18));
  } else
    (void)(gen_primitive_type(kind));
}
int gen_scalar_kind(int arg) {
  if (arg != 0) {
    int typed = tc_emit_arg_type(arg);
    if (typed != 0) {
      int resolved = gen_substitute_type(typed);
      if ((resolved != 0) && (node_kind[resolved] != TY_PARAM))
        return node_kind[resolved];
      else {
      }
    } else {
    }
    if (node_kind[arg] == N_INT)
      return TY_INT;
    else {
    }
    if (node_kind[arg] == N_BOOL)
      return TY_BOOL;
    else {
    }
    if (node_kind[arg] == N_CHAR)
      return TY_CHAR;
    else {
    }
    if (node_kind[arg] == N_FLOAT)
      return TY_DOUBLE;
    else {
    }
    if (node_kind[arg] == N_STRING)
      return TY_STRING;
    else {
    }
    if (node_kind[arg] == N_VAR)
      return sym_type[node_value[arg]];
    else {
    }
  } else {
  }
  return gen_expr_kind(arg);
}
int gen_scalar_name(int arg) {
  if (arg != 0) {
    int typed = tc_emit_arg_type(arg);
    if (typed != 0) {
      int resolved = gen_substitute_type(typed);
      if ((resolved != 0) && (node_kind[resolved] == TY_NAMED))
        return node_value[resolved];
      else {
      }
    } else {
    }
    if (node_kind[arg] == N_VAR)
      return sym_elem_name[node_value[arg]];
    else {
    }
  } else {
  }
  return 0;
}
int gen_array_elem_kind(int arg) {
  if ((arg != 0) && (node_kind[arg] == N_VAR)) {
    int name = node_value[arg];
    int formal_type = gen_active_param_type(name);
    if (formal_type != 0) {
      int resolved_type = gen_substitute_type(formal_type);
      if (((resolved_type != 0) && (node_kind[resolved_type] == TY_DYN_ARRAY)) &&
          (node_a[resolved_type] != 0))
        return node_kind[node_a[resolved_type]];
      else {
      }
    } else {
    }
    int kind = sym_elem_kind[name];
    int elem_name = sym_elem_name[name];
    int param_name = elem_name;
    if (((kind == TY_PARAM) && (elem_name != 0)) && (node_kind[elem_name] == TY_PARAM))
      param_name = node_value[elem_name];
    else {
    }
    if (kind == TY_PARAM) {
      int bound = gen_bind_find(param_name);
      if (bound != 0) {
        int resolved = gen_substitute_type(bound);
        if (resolved != 0)
          return node_kind[resolved];
        else {
        }
      } else {
      }
    } else {
    }
    return kind;
  } else {
  }
  return TY_INT;
}
int gen_array_elem_name(int arg) {
  if ((arg != 0) && (node_kind[arg] == N_VAR)) {
    int name = node_value[arg];
    int formal_type = gen_active_param_type(name);
    if (formal_type != 0) {
      int resolved_type = gen_substitute_type(formal_type);
      if ((((resolved_type != 0) && (node_kind[resolved_type] == TY_DYN_ARRAY)) &&
           (node_a[resolved_type] != 0)) &&
          (node_kind[node_a[resolved_type]] == TY_NAMED))
        return node_value[node_a[resolved_type]];
      else {
      }
    } else {
    }
    int kind = sym_elem_kind[name];
    int elem_name = sym_elem_name[name];
    int param_name = elem_name;
    if (((kind == TY_PARAM) && (elem_name != 0)) && (node_kind[elem_name] == TY_PARAM))
      param_name = node_value[elem_name];
    else {
    }
    if (kind == TY_PARAM) {
      int bound = gen_bind_find(param_name);
      if (bound != 0) {
        int resolved = gen_substitute_type(bound);
        if ((resolved != 0) && (node_kind[resolved] == TY_NAMED))
          return node_value[resolved];
        else {
        }
      } else {
      }
      return 0;
    } else {
    }
    return elem_name;
  } else {
  }
  return 0;
}
void gen_array_elem_type(int kind, int name) {
  if (kind == TY_STRING) {
    (void)(code_emit(C_KW, 17));
    (void)(code_emit(C_PUNCT, 1));
  } else if (kind == TY_NAMED)
    (void)(code_emit(C_IDENT, sym_c_symbol(name)));
  else if (kind == TY_PTR) {
    (void)(code_emit(C_KW, 1));
    (void)(code_emit(C_PUNCT, 1));
  } else if (kind == TY_PARAM)
    (void)(code_emit(C_KW, 1));
  else
    (void)(gen_primitive_type(kind));
}
void gen_array_sizeof(int kind, int name) {
  (void)(code_emit(C_IDENT, (0 - 1011)));
  (void)(code_emit(C_PUNCT, 4));
  (void)(gen_array_elem_type(kind, name));
  (void)(code_emit(C_PUNCT, 5));
}
void gen_array_value_ptr(int kind, int name, int value) {
  (void)(code_emit(C_PUNCT, 10));
  (void)(code_emit(C_PUNCT, 4));
  (void)(gen_array_elem_type(kind, name));
  if (kind == TY_NAMED) {
    (void)(code_emit(C_PUNCT, 2));
    (void)(code_emit(C_PUNCT, 3));
  } else {
  }
  (void)(code_emit(C_PUNCT, 5));
  (void)(code_emit(C_PUNCT, 24));
  if (kind == TY_FLOAT) {
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_KW, 18));
    (void)(code_emit(C_PUNCT, 5));
  } else {
  }
  (void)(gen_expr(value));
  (void)(code_emit(C_PUNCT, 25));
}
void gen_array_sizeof_node(int ty) {
  (void)(code_emit(C_IDENT, (0 - 1011)));
  (void)(code_emit(C_PUNCT, 4));
  if ((ty != 0) && (node_kind[ty] == TY_GENERIC))
    (void)(code_emit(C_IDENT, gen_mangled_type_symbol(ty)));
  else if ((ty != 0) && (node_kind[ty] == TY_PTR))
    (void)(gen_type(node_kind[ty], ty, 0));
  else if (ty != 0)
    (void)(gen_array_elem_type(node_kind[ty], node_value[ty]));
  else
    (void)(gen_array_elem_type(TY_INT, 0));
  (void)(code_emit(C_PUNCT, 5));
}
void gen_memory_sizeof(int arg) {
  int ty = tc_emit_arg_type(arg);
  if (ty != 0)
    ty = gen_substitute_type(ty);
  else {
  }
  if (((ty != 0) && (node_kind[ty] == TY_PTR)) && (node_a[ty] != 0)) {
    int elem = gen_substitute_type(node_a[ty]);
    (void)(gen_array_sizeof_node(elem));
  } else if (ty != 0)
    (void)(gen_array_sizeof_node(ty));
  else
    (void)(gen_array_sizeof_node(0));
}
void gen_memory_builtin(int id) {
  int call_name = node_value[id];
  int btag = bi_tag(call_name);
  int a = node_a[id];
  if ((btag == BI_TC_MEM_ALLOC) || (btag == BI_TC_MEM_ALLOC_ALIGNED)) {
    int alignment_arg = 0;
    int witness = node_next[a];
    if (btag == BI_TC_MEM_ALLOC_ALIGNED) {
      alignment_arg = witness;
      witness = node_next[witness];
    } else {
    }
    int elem_kind = gen_scalar_kind(witness);
    int elem_name = gen_scalar_name(witness);
    int gen_witness_ty = 0;
    int complex_witness_ty = 0;
    if (elem_kind == TY_GENERIC) {
      int wty = tc_emit_arg_type(witness);
      if (wty != 0)
        gen_witness_ty = gen_substitute_type(wty);
      else {
      }
    } else if (elem_kind == TY_PTR) {
      int pointer_witness_ty = tc_emit_arg_type(witness);
      if (pointer_witness_ty != 0)
        complex_witness_ty = gen_substitute_type(pointer_witness_ty);
      else {
      }
    } else {
    }
    (void)(code_emit(C_PUNCT, 6));
    (void)(code_emit(C_PUNCT, 6));
    (void)(code_emit(C_KW, 4));
    (void)(code_emit(C_PUNCT, 8));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_expr(witness));
    (void)(code_emit(C_PUNCT, 8));
    (void)(code_emit(C_PUNCT, 7));
    (void)(code_emit(C_PUNCT, 6));
    if (gen_witness_ty != 0)
      (void)(code_emit(C_IDENT, gen_mangled_type_symbol(gen_witness_ty)));
    else if (complex_witness_ty != 0)
      (void)(gen_type(node_kind[complex_witness_ty], complex_witness_ty, 0));
    else
      (void)(gen_array_elem_type(elem_kind, elem_name));
    (void)(code_emit(C_PUNCT, 1));
    (void)(code_emit(C_PUNCT, 8));
    if (btag == BI_TC_MEM_ALLOC_ALIGNED)
      (void)(code_emit(C_IDENT, (0 - 1019)));
    else
      (void)(code_emit(C_IDENT, (0 - 1016)));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_expr(a));
    (void)(code_emit(C_PUNCT, 7));
    if (btag == BI_TC_MEM_ALLOC_ALIGNED) {
      (void)(gen_expr(alignment_arg));
      (void)(code_emit(C_PUNCT, 7));
    } else {
    }
    if (gen_witness_ty != 0) {
      (void)(code_emit(C_IDENT, (0 - 1011)));
      (void)(code_emit(C_PUNCT, 4));
      (void)(code_emit(C_IDENT, gen_mangled_type_symbol(gen_witness_ty)));
      (void)(code_emit(C_PUNCT, 5));
    } else if (complex_witness_ty != 0) {
      (void)(code_emit(C_IDENT, (0 - 1011)));
      (void)(code_emit(C_PUNCT, 4));
      (void)(gen_type(node_kind[complex_witness_ty], complex_witness_ty, 0));
      (void)(code_emit(C_PUNCT, 5));
    } else
      (void)(gen_array_sizeof(elem_kind, elem_name));
    (void)(code_emit(C_PUNCT, 8));
    (void)(code_emit(C_PUNCT, 8));
  } else if (btag == BI_TC_MEM_RESIZE) {
    int old_count = node_next[a];
    int new_count = node_next[old_count];
    int zero = node_next[new_count];
    int ptr_ty = tc_emit_arg_type(a);
    if (ptr_ty != 0)
      ptr_ty = gen_substitute_type(ptr_ty);
    else {
    }
    if (ptr_ty == 0)
      ptr_ty = ast_node(TY_PTR, ast_node(TY_INT, 0, 0, 0, 0, 0), 0, 0, 0, 0);
    else {
    }
    (void)(code_emit(C_PUNCT, 6));
    (void)(code_emit(C_PUNCT, 6));
    (void)(code_emit(C_KW, 4));
    (void)(code_emit(C_PUNCT, 8));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_expr(zero));
    (void)(code_emit(C_PUNCT, 8));
    (void)(code_emit(C_PUNCT, 7));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_type(node_kind[ptr_ty], ptr_ty, 0));
    (void)(code_emit(C_PUNCT, 8));
    (void)(code_emit(C_IDENT, (0 - 1017)));
    (void)(code_emit(C_PUNCT, 6));
    (void)(code_emit(C_PUNCT, 6));
    (void)(code_emit(C_KW, 4));
    (void)(code_emit(C_PUNCT, 1));
    (void)(code_emit(C_PUNCT, 8));
    (void)(gen_expr(a));
    (void)(code_emit(C_PUNCT, 7));
    (void)(gen_expr(old_count));
    (void)(code_emit(C_PUNCT, 7));
    (void)(gen_expr(new_count));
    (void)(code_emit(C_PUNCT, 7));
    (void)(gen_memory_sizeof(a));
    (void)(code_emit(C_PUNCT, 8));
    (void)(code_emit(C_PUNCT, 8));
  } else if (btag == BI_TC_MEM_FREE) {
    (void)(code_emit(C_IDENT, (0 - 1018)));
    (void)(code_emit(C_PUNCT, 6));
    (void)(code_emit(C_PUNCT, 6));
    (void)(code_emit(C_KW, 4));
    (void)(code_emit(C_PUNCT, 1));
    (void)(code_emit(C_PUNCT, 8));
    (void)(gen_expr(a));
    (void)(code_emit(C_PUNCT, 8));
  } else {
    (void)(code_emit(C_IDENT, sym_c_symbol(call_name)));
    (void)(code_emit(C_PUNCT, 6));
    int arg = a;
    while (arg != 0) {
      (void)(gen_expr(arg));
      if (node_next[arg] != 0)
        (void)(code_emit(C_PUNCT, 7));
      else {
      }
      arg = node_next[arg];
    }
    (void)(code_emit(C_PUNCT, 8));
  }
}
int gen_call_name(int id) {
  int f = tc_find_function_ctx(node_value[id], node_scope[id]);
  if (f == 0)
    return sym_c_symbol(node_value[id]);
  else {
  }
  if (node_kind[f] != N_GENERIC_FUNC)
    return sym_c_symbol(node_value[f]);
  else {
  }
  int actual = node_b[id];
  int a = node_a[id];
  if (actual == 0) {
    while (a != 0) {
      int q = 0;
      q = tc_emit_arg_type(a);
      if (q == 0) {
        (void)(tc_expr(a));
        q = tc_result_type;
        if (q == 0)
          q = tc_type_node_from_summary(tc_kind, tc_name, tc_elem_kind, tc_elem_name);
        else {
        }
      } else {
      }
      if (q != 0)
        q = gen_substitute_type(q);
      else {
      }
      if (actual == 0)
        actual = q;
      else
        actual = ast_link(actual, q);
      a = node_next[a];
    }
  } else {
  }
  int saved_count = gen_bind_count;
  (void)(ensure_gen_bind((saved_count + saved_count)));
  int save_i = 0;
  while (save_i < saved_count) {
    gen_bind_name[(saved_count + save_i)] = gen_bind_name[save_i];
    gen_bind_type[(saved_count + save_i)] = gen_bind_type[save_i];
    save_i = (save_i + 1);
  }
  (void)(gen_bind_decl(f, actual));
  int typeargs = 0;
  int tp = node_aux[f];
  while (tp != 0) {
    int bt = gen_bind_find(node_a[tp]);
    if (bt == 0)
      bt = ast_node(TY_PARAM, 0, 0, 0, node_a[tp], 0);
    else {
    }
    int cq = gen_substitute_type(bt);
    if (typeargs == 0)
      typeargs = cq;
    else
      typeargs = ast_link(typeargs, cq);
    tp = node_next[tp];
  }
  (void)(gen_bind_clear());
  int restore_i = 0;
  while (restore_i < saved_count) {
    gen_bind_name[restore_i] = gen_bind_name[(saved_count + restore_i)];
    gen_bind_type[restore_i] = gen_bind_type[(saved_count + restore_i)];
    restore_i = (restore_i + 1);
  }
  gen_bind_count = saved_count;
  return gen_mangled_function_symbol(node_value[f], typeargs);
}
void gen_variant_expr(int id) {
  (void)(tc_find_enum_variant(node_value[id]));
  int enum_name = tc_variant_enum;
  if (((enum_name == 0) && (node_aux[id] != 0)) && (node_kind[node_aux[id]] == TY_NAMED))
    enum_name = node_value[node_aux[id]];
  else {
  }
  int member = tc_variant_member;
  if (member == 0) {
    (void)(code_emit(C_INT, 0));
    return;
  } else {
  }
  int enum_decl = tc_find_enum(enum_name);
  int tagged = 0;
  if (enum_decl != 0) {
    int probe = node_a[enum_decl];
    while (probe != 0) {
      if (node_b[probe] != 0)
        tagged = 1;
      else {
      }
      probe = node_next[probe];
    }
  } else {
  }
  if (tagged == 0) {
    (void)(code_emit(C_IDENT, sym_c_symbol(sym_qualified(enum_name, node_a[member]))));
    return;
  } else {
  }
  (void)(code_emit(C_PUNCT, 4));
  (void)(code_emit(C_IDENT, sym_c_symbol(enum_name)));
  (void)(code_emit(C_PUNCT, 5));
  (void)(code_emit(C_PUNCT, 24));
  (void)(code_emit(C_PUNCT, 17));
  (void)(code_emit(C_IDENT, sym_tag_id()));
  (void)(code_emit(C_PUNCT, 11));
  (void)(code_emit(C_IDENT, sym_c_symbol(sym_qualified(enum_name, node_a[member]))));
  int payload = node_b[member];
  if (payload != 0) {
    (void)(code_emit(C_PUNCT, 7));
    (void)(code_emit(C_PUNCT, 17));
    (void)(code_emit(C_IDENT, sym_c_symbol(node_a[member])));
    (void)(code_emit(C_PUNCT, 11));
    (void)(code_emit(C_PUNCT, 24));
    int arg = node_a[id];
    int field = payload;
    while ((field != 0) && (arg != 0)) {
      (void)(code_emit(C_PUNCT, 17));
      (void)(code_emit(C_IDENT, sym_c_symbol(node_a[field])));
      (void)(code_emit(C_PUNCT, 11));
      (void)(gen_expr(arg));
      if ((node_next[field] != 0) && (node_next[arg] != 0))
        (void)(code_emit(C_PUNCT, 7));
      else {
      }
      field = node_next[field];
      arg = node_next[arg];
    }
    (void)(code_emit(C_PUNCT, 25));
  } else {
  }
  (void)(code_emit(C_PUNCT, 25));
}
void gen_expr(int id) {
  int k = node_kind[id];
  if (k == N_INT) {
    if (node_aux[id] != 0) {
      if (node_value[id] < 0)
        (void)(code_emit(C_RAW_U64, node_aux[id]));
      else
        (void)(code_emit(C_RAW, node_aux[id]));
    } else
      (void)(code_emit(C_INT, node_value[id]));
  } else if (k == N_BOOL)
    (void)(code_emit(C_INT, node_value[id]));
  else if (k == N_FLOAT) {
    (void)(code_emit(C_IDENT, node_value[id]));
    if (node_aux[id] == TY_FLOAT)
      (void)(code_emit(C_IDENT, (0 - 1021)));
    else {
    }
  } else if (k == N_STRING)
    (void)(code_emit(C_STRING, node_value[id]));
  else if (k == N_CHAR)
    (void)(code_emit(C_INT, node_value[id]));
  else if (k == N_VARIANT)
    (void)(gen_variant_expr(id));
  else if (k == N_TUPLE) {
    int tuple_ty = node_aux[id];
    if (tuple_ty == 0) {
      (void)(tc_expr(id));
      tuple_ty = tc_result_type;
    } else {
    }
    if (tuple_ty == 0) {
      (void)(code_emit(C_PUNCT, 24));
      (void)(code_emit(C_PUNCT, 25));
    } else {
      (void)(code_emit(C_PUNCT, 4));
      (void)(code_emit(C_KW, 12));
      (void)(code_emit(C_IDENT, gen_mangled_type_symbol(tuple_ty)));
      (void)(code_emit(C_PUNCT, 5));
      (void)(code_emit(C_PUNCT, 24));
      int item = node_a[id];
      int index = 0;
      while (item != 0) {
        (void)(code_emit(C_PUNCT, 17));
        (void)(code_emit(C_IDENT, gen_tuple_field_name(index)));
        (void)(code_emit(C_PUNCT, 11));
        (void)(gen_expr(item));
        if (node_next[item] != 0)
          (void)(code_emit(C_PUNCT, 7));
        else {
        }
        item = node_next[item];
        index = (index + 1);
      }
      (void)(code_emit(C_PUNCT, 25));
    }
  } else if (k == N_NULL) {
    (void)(code_emit(C_INT, 0));
  } else if (k == N_MOVE) {
    (void)(gen_expr(node_a[id]));
  } else if (k == N_CLOSURE) {
    int closure_serial_expr = (node_aux[id] - 1);
    (void)(code_emit(C_IDENT, gen_closure_factory_name(closure_serial_expr)));
    (void)(code_emit(C_PUNCT, 6));
    int closure_cap_expr = node_a[id];
    while (closure_cap_expr != 0) {
      int capture_var_expr = ast_node(N_VAR, 0, 0, 0, node_a[closure_cap_expr], 0);
      if ((node_aux[closure_cap_expr] == 2) || (node_aux[closure_cap_expr] == 3))
        (void)(code_emit(C_PUNCT, 10));
      else {
      }
      (void)(gen_expr(capture_var_expr));
      if (node_next[closure_cap_expr] != 0)
        (void)(code_emit(C_PUNCT, 7));
      else {
      }
      closure_cap_expr = node_next[closure_cap_expr];
    }
    (void)(code_emit(C_PUNCT, 8));
  } else if (k == N_UNARY) {
    (void)(code_emit(C_OP, OP_NOT));
    (void)(code_emit(C_PUNCT, 4));
    (void)(gen_expr(node_a[id]));
    (void)(code_emit(C_PUNCT, 5));
  } else if (k == N_VAR) {
    int active_capture = gen_active_closure_capture(node_value[id]);
    if ((active_capture != 0) && (node_aux[active_capture] != 1)) {
      (void)(code_emit(C_PUNCT, 4));
      (void)(code_emit(C_PUNCT, 9));
      (void)(code_emit(C_IDENT, gen_closure_active_env_name()));
      (void)(code_emit(C_PUNCT, 27));
      (void)(code_emit(C_IDENT, sym_c_symbol(node_a[active_capture])));
      (void)(code_emit(C_PUNCT, 5));
    } else if ((node_aux[id] != 0) && (node_kind[node_aux[id]] == TY_NAMED)) {
      int enum_name = node_value[node_aux[id]];
      int enum_decl = tc_find_enum(enum_name);
      int member = tc_match_variant_member(enum_decl, node_value[id]);
      if (member != 0) {
        int tagged = 0;
        int probe = node_a[enum_decl];
        while (probe != 0) {
          if (node_b[probe] != 0)
            tagged = 1;
          else {
          }
          probe = node_next[probe];
        }
        if (tagged == 0)
          (void)(code_emit(C_IDENT, sym_c_symbol(sym_qualified(enum_name, node_a[member]))));
        else {
          (void)(code_emit(C_PUNCT, 4));
          (void)(code_emit(C_IDENT, sym_c_symbol(enum_name)));
          (void)(code_emit(C_PUNCT, 5));
          (void)(code_emit(C_PUNCT, 24));
          (void)(code_emit(C_PUNCT, 17));
          (void)(code_emit(C_IDENT, sym_tag_id()));
          (void)(code_emit(C_PUNCT, 11));
          (void)(code_emit(C_IDENT, sym_c_symbol(sym_qualified(enum_name, node_a[member]))));
          (void)(code_emit(C_PUNCT, 25));
        }
      } else
        (void)(code_emit(C_IDENT, sym_c_symbol(node_value[id])));
    } else
      (void)(code_emit(C_IDENT, sym_c_symbol(node_value[id])));
  } else if (k == N_BINOP) {
    if (node_c[id] == TY_FLOAT) {
      (void)(code_emit(C_PUNCT, 4));
      (void)(code_emit(C_KW, 18));
      (void)(code_emit(C_PUNCT, 5));
      (void)(code_emit(C_PUNCT, 4));
    } else {
    }
    if (node_value[id] == OP_CONCAT) {
      (void)(code_emit(C_IDENT, (0 - 1002)));
      (void)(code_emit(C_PUNCT, 6));
      (void)(gen_expr(node_a[id]));
      (void)(code_emit(C_PUNCT, 7));
      (void)(gen_expr(node_b[id]));
      (void)(code_emit(C_PUNCT, 8));
    } else if (((node_value[id] == OP_SUB) && (gen_expr_kind(node_a[id]) == TY_PTR)) &&
               (gen_expr_kind(node_b[id]) == TY_PTR)) {
      (void)(code_emit(C_PUNCT, 4));
      (void)(code_emit(C_KW, 1));
      (void)(code_emit(C_PUNCT, 5));
      (void)(code_emit(C_PUNCT, 4));
      (void)(gen_expr(node_a[id]));
      (void)(code_emit(C_OP, node_value[id]));
      (void)(gen_expr(node_b[id]));
      (void)(code_emit(C_PUNCT, 5));
    } else {
      (void)(code_emit(C_PUNCT, 4));
      (void)(gen_expr(node_a[id]));
      (void)(code_emit(C_OP, node_value[id]));
      (void)(gen_expr(node_b[id]));
      (void)(code_emit(C_PUNCT, 5));
    }
    if (node_c[id] == TY_FLOAT)
      (void)(code_emit(C_PUNCT, 5));
    else {
    }
  } else if (k == N_CALL) {
    int call_name = node_value[id];
    int btag = bi_tag(call_name);
    if ((((btag == BI_TC_MEM_ALLOC) || (btag == BI_TC_MEM_ALLOC_ALIGNED)) ||
         (btag == BI_TC_MEM_RESIZE)) ||
        (btag == BI_TC_MEM_FREE))
      (void)(gen_memory_builtin(id));
    else {
      (void)(code_emit(C_IDENT, gen_call_name(id)));
      (void)(code_emit(C_PUNCT, 6));
      int arg = node_a[id];
      while (arg != 0) {
        (void)(gen_expr(arg));
        if (node_next[arg] != 0)
          (void)(code_emit(C_PUNCT, 7));
        else {
        }
        arg = node_next[arg];
      }
      (void)(code_emit(C_PUNCT, 8));
    }
  } else if (k == N_INDIRECT_CALL) {
    int callee_type_expr = tc_emit_arg_type(node_a[id]);
    if ((callee_type_expr != 0) && (node_kind[callee_type_expr] == TY_CLOSURE)) {
      (void)(code_emit(C_IDENT, gen_closure_call_name(callee_type_expr)));
      (void)(code_emit(C_PUNCT, 6));
      (void)(gen_expr(node_a[id]));
      int closure_arg_emit = node_b[id];
      if (closure_arg_emit != 0)
        (void)(code_emit(C_PUNCT, 7));
      else {
      }
      while (closure_arg_emit != 0) {
        (void)(gen_expr(closure_arg_emit));
        if (node_next[closure_arg_emit] != 0)
          (void)(code_emit(C_PUNCT, 7));
        else {
        }
        closure_arg_emit = node_next[closure_arg_emit];
      }
      (void)(code_emit(C_PUNCT, 8));
    } else {
      (void)(code_emit(C_PUNCT, 4));
      (void)(gen_expr(node_a[id]));
      (void)(code_emit(C_PUNCT, 5));
      (void)(code_emit(C_PUNCT, 6));
      int arg = node_b[id];
      while (arg != 0) {
        (void)(gen_expr(arg));
        if (node_next[arg] != 0)
          (void)(code_emit(C_PUNCT, 7));
        else {
        }
        arg = node_next[arg];
      }
      (void)(code_emit(C_PUNCT, 8));
    }
  } else if (k == N_DEREF) {
    (void)(code_emit(C_PUNCT, 9));
    (void)(gen_expr(node_a[id]));
  } else if (k == N_ADDRESS) {
    (void)(code_emit(C_PUNCT, 10));
    (void)(gen_expr(node_a[id]));
  } else if (k == N_INDEX) {
    (void)(gen_expr(node_a[id]));
    (void)(code_emit(C_PUNCT, 2));
    (void)(gen_expr(node_b[id]));
    (void)(code_emit(C_PUNCT, 3));
  } else if (k == N_FIELD_ACCESS) {
    (void)(gen_expr(node_a[id]));
    if (gen_expr_kind(node_a[id]) == TY_PTR)
      (void)(code_emit(C_PUNCT, 27));
    else
      (void)(code_emit(C_PUNCT, 17));
    (void)(code_emit(C_IDENT, node_value[id]));
  } else {
  }
}
void gen_expr_condition_inner(int id) {
  if (node_kind[id] != N_BINOP) {
    (void)(gen_expr(id));
    return;
  } else {
  }
  if (node_value[id] == OP_CONCAT) {
    (void)(code_emit(C_IDENT, (0 - 1002)));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_expr(node_a[id]));
    (void)(code_emit(C_PUNCT, 7));
    (void)(gen_expr(node_b[id]));
    (void)(code_emit(C_PUNCT, 8));
    return;
  } else {
  }
  if (((node_value[id] == OP_SUB) && (gen_expr_kind(node_a[id]) == TY_PTR)) &&
      (gen_expr_kind(node_b[id]) == TY_PTR)) {
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_KW, 1));
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 4));
    (void)(gen_expr(node_a[id]));
    (void)(code_emit(C_OP, node_value[id]));
    (void)(gen_expr(node_b[id]));
    (void)(code_emit(C_PUNCT, 5));
    return;
  } else {
  }
  (void)(gen_expr(node_a[id]));
  (void)(code_emit(C_OP, node_value[id]));
  (void)(gen_expr(node_b[id]));
}
int gen_expr_kind(int id) {
  int k = node_kind[id];
  if ((k == N_INT) || (k == N_BOOL))
    return TY_INT;
  else {
  }
  if (k == N_CLOSURE)
    return TY_CLOSURE;
  else {
  }
  if (k == N_MOVE)
    return gen_expr_kind(node_a[id]);
  else {
  }
  if (k == N_TUPLE)
    return TY_TUPLE;
  else {
  }
  if (k == N_VARIANT) {
    if (node_aux[id] != 0)
      return node_kind[node_aux[id]];
    else {
    }
    return TY_NAMED;
  } else {
  }
  if (k == N_CHAR)
    return TY_CHAR;
  else {
  }
  if (k == N_FLOAT) {
    if (node_aux[id] == TY_FLOAT)
      return TY_FLOAT;
    else {
    }
    return TY_DOUBLE;
  } else {
  }
  if (k == N_STRING)
    return TY_STRING;
  else {
  }
  if (k == N_NULL)
    return TY_PTR;
  else {
  }
  if (k == N_UNARY)
    return TY_BOOL;
  else {
  }
  if (k == N_VAR) {
    int stable_type = node_type[id];
    if ((stable_type != 0) && (node_kind[stable_type] != TY_PARAM))
      return node_kind[stable_type];
    else {
    }
    int formal_type = gen_active_param_type(node_value[id]);
    if (formal_type != 0) {
      int resolved_type = gen_substitute_type(formal_type);
      if (resolved_type != 0)
        return node_kind[resolved_type];
      else {
      }
    } else {
    }
    int vt = sym_type[node_value[id]];
    if (vt > 99) {
      tc_elem_kind = sym_elem_kind[node_value[id]];
      tc_elem_name = sym_elem_name[node_value[id]];
      return (vt - 100);
    } else {
    }
    return vt;
  } else {
  }
  if (k == N_INDEX) {
    int bt = gen_expr_kind(node_a[id]);
    if (bt == TY_STRING)
      return TY_CHAR;
    else {
    }
    int index_type = tc_emit_arg_type(node_a[id]);
    if ((index_type != 0) &&
        (((node_kind[index_type] == TY_PTR) || (node_kind[index_type] == TY_ARRAY)) ||
         (node_kind[index_type] == TY_DYN_ARRAY))) {
      int index_elem = node_a[index_type];
      if (index_elem != 0)
        return node_kind[index_elem];
      else {
      }
    } else {
    }
    if (bt == TY_PTR)
      return TY_INT;
    else {
    }
    return TY_INT;
  } else {
  }
  if (k == N_DEREF) {
    int deref_type = tc_emit_arg_type(node_a[id]);
    if (((deref_type != 0) && (node_kind[deref_type] == TY_PTR)) && (node_a[deref_type] != 0))
      return node_kind[node_a[deref_type]];
    else {
    }
    return TY_INT;
  } else {
  }
  if (k == N_ADDRESS)
    return TY_PTR;
  else {
  }
  if (k == N_FIELD_ACCESS) {
    int field_ty = tc_emit_field_type(id);
    if (field_ty != 0)
      return node_kind[field_ty];
    else {
    }
    return TY_INT;
  } else {
  }
  if ((k == N_CALL) || (k == N_INDIRECT_CALL))
    return tc_expr_kind_for_emit(id);
  else {
  }
  if (k == N_BINOP) {
    if (node_value[id] == OP_CONCAT)
      return TY_STRING;
    else {
    }
    if ((((((((node_value[id] == OP_EQ) || (node_value[id] == OP_NEQ)) ||
             (node_value[id] == OP_LT)) ||
            (node_value[id] == OP_GT)) ||
           (node_value[id] == OP_LE)) ||
          (node_value[id] == OP_GE)) ||
         (node_value[id] == OP_AND)) ||
        (node_value[id] == OP_OR))
      return TY_BOOL;
    else {
    }
    if (((node_value[id] == OP_SUB) && (gen_expr_kind(node_a[id]) == TY_PTR)) &&
        (gen_expr_kind(node_b[id]) == TY_PTR))
      return TY_INT;
    else {
    }
    int ak = gen_expr_kind(node_a[id]);
    int bk = gen_expr_kind(node_b[id]);
    if (((((node_value[id] == OP_BITAND) || (node_value[id] == OP_BITOR)) ||
          (node_value[id] == OP_BITXOR)) ||
         (node_value[id] == OP_SHL)) ||
        (node_value[id] == OP_SHR)) {
      if ((ak == TY_LLONG) || (bk == TY_LLONG))
        return TY_LLONG;
      else {
      }
      if ((ak == TY_LONG) || (bk == TY_LONG))
        return TY_LONG;
      else {
      }
      return TY_INT;
    } else {
    }
    if ((ak == TY_DOUBLE) || (bk == TY_DOUBLE))
      return TY_DOUBLE;
    else {
    }
    if ((ak == TY_FLOAT) || (bk == TY_FLOAT))
      return TY_FLOAT;
    else {
    }
    if ((ak == TY_LLONG) || (bk == TY_LLONG))
      return TY_LLONG;
    else {
    }
    if ((ak == TY_LONG) || (bk == TY_LONG))
      return TY_LONG;
    else {
    }
    if (ak == TY_PTR)
      return TY_PTR;
    else {
    }
    return TY_INT;
  } else {
  }
  return TY_INT;
}
void gen_initializer(int ty, int expr) {
  int st = gen_substitute_type(ty);
  if (((((node_kind[st] == TY_NAMED) || (node_kind[st] == TY_GENERIC)) ||
        (node_kind[st] == TY_ARRAY)) &&
       (node_kind[expr] == N_INT)) &&
      (node_value[expr] == 0))
    (void)(code_emit(C_PUNCT, 19));
  else if ((node_kind[st] == TY_FLOAT) && (gen_expr_kind(expr) != TY_FLOAT)) {
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_KW, 18));
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 4));
    (void)(gen_expr(expr));
    (void)(code_emit(C_PUNCT, 5));
  } else
    (void)(gen_expr(expr));
}
void gen_assignment(int lhs, int rhs) {
  (void)(gen_expr(lhs));
  (void)(code_emit(C_PUNCT, 11));
  if ((gen_expr_kind(lhs) == TY_FLOAT) && (gen_expr_kind(rhs) != TY_FLOAT)) {
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_KW, 18));
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 4));
    (void)(gen_expr(rhs));
    (void)(code_emit(C_PUNCT, 5));
  } else
    (void)(gen_expr(rhs));
}
int compound_c_operator(int op) {
  if (op == OP_ADD)
    return 19;
  else {
  }
  if (op == OP_SUB)
    return 20;
  else {
  }
  if (op == OP_MUL)
    return 21;
  else {
  }
  if (op == OP_DIV)
    return 22;
  else {
  }
  if (op == OP_MOD)
    return 23;
  else {
  }
  if (op == OP_BITAND)
    return 24;
  else {
  }
  if (op == OP_BITOR)
    return 25;
  else {
  }
  if (op == OP_BITXOR)
    return 26;
  else {
  }
  if (op == OP_SHL)
    return 27;
  else {
  }
  if (op == OP_SHR)
    return 28;
  else {
  }
  return 19;
}
void gen_compound_assignment(int lhs, int op, int rhs) {
  (void)(gen_expr(lhs));
  (void)(code_emit(C_OP, compound_c_operator(op)));
  if ((gen_expr_kind(lhs) == TY_FLOAT) && (gen_expr_kind(rhs) != TY_FLOAT)) {
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_KW, 18));
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 4));
    (void)(gen_expr(rhs));
    (void)(code_emit(C_PUNCT, 5));
  } else
    (void)(gen_expr(rhs));
}
void gen_for_clause(int id) {
  if (id == 0)
    return;
  else {
  }
  if (node_kind[id] == N_LET) {
    int for_type = gen_substitute_type(node_b[id]);
    (void)(gen_type(node_kind[for_type], for_type, node_value[for_type]));
    (void)(code_emit(C_IDENT, node_a[id]));
    (void)(code_emit(C_PUNCT, 11));
    (void)(gen_initializer(for_type, node_c[id]));
  } else if (node_kind[id] == N_ASSIGN) {
    (void)(gen_assignment(node_a[id], node_b[id]));
  } else if (node_kind[id] == N_COMPOUND_ASSIGN) {
    (void)(gen_compound_assignment(node_a[id], node_value[id], node_b[id]));
  } else if (node_kind[id] == N_EXPR)
    (void)(gen_expr(node_a[id]));
  else {
  }
}
void ensure_gen_fun_chain(int need) {
  if (need < gen_fun_chain_cap)
    return;
  else {
  }
  int n = next_capacity(gen_fun_chain_cap, need);
  gen_fun_chain_type = grow_ints(gen_fun_chain_type, gen_fun_chain_cap, n);
  gen_fun_chain_cap = n;
}
void gen_fun_decl(int ty, int name) {
  gen_fun_chain_count = 0;
  int current = gen_substitute_type(ty);
  while ((current != 0) && (node_kind[current] == TY_FUN)) {
    (void)(ensure_gen_fun_chain(gen_fun_chain_count));
    gen_fun_chain_type[gen_fun_chain_count] = current;
    gen_fun_chain_count = (gen_fun_chain_count + 1);
    current = gen_substitute_type(node_b[current]);
  }
  (void)(gen_type(node_kind[current], current, node_value[current]));
  int prefix = 0;
  while (prefix < gen_fun_chain_count) {
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_PUNCT, 9));
    prefix = (prefix + 1);
  }
  (void)(code_emit(C_IDENT, sym_c_symbol(name)));
  int suffix = 0;
  while (suffix < gen_fun_chain_count) {
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_function_type_params(node_a[gen_fun_chain_type[suffix]]));
    (void)(code_emit(C_PUNCT, 8));
    suffix = (suffix + 1);
  }
}
void gen_const_decl(int ty, int name) {
  if (node_kind[ty] == TY_PTR) {
    int pointee = node_a[ty];
    (void)(gen_type(node_kind[pointee], pointee, node_value[pointee]));
    (void)(code_emit(C_PUNCT, 1));
    (void)(code_emit(C_KW, 16));
    (void)(code_emit(C_IDENT, sym_c_symbol(name)));
  } else {
    (void)(code_emit(C_KW, 16));
    (void)(gen_decl(ty, name));
  }
}
void gen_array_base_type(int ty) {
  if (node_kind[ty] == TY_ARRAY)
    (void)(gen_array_base_type(node_a[ty]));
  else
    (void)(gen_type(node_kind[ty], ty, node_value[ty]));
}
void gen_array_suffixes(int ty) {
  if (node_kind[ty] == TY_ARRAY) {
    (void)(code_emit(C_PUNCT, 2));
    (void)(code_emit(C_INT, node_value[ty]));
    (void)(code_emit(C_PUNCT, 3));
    (void)(gen_array_suffixes(node_a[ty]));
  } else {
  }
}
void gen_decl(int ty, int name) {
  if (node_kind[ty] == TY_FUN)
    (void)(gen_fun_decl(ty, name));
  else if (node_kind[ty] == TY_ARRAY) {
    int array_base = ty;
    while (node_kind[array_base] == TY_ARRAY) {
      array_base = node_a[array_base];
    }
    if (node_kind[array_base] == TY_FUN) {
      int function_return = gen_substitute_type(node_b[array_base]);
      (void)(gen_type(node_kind[function_return], function_return, node_value[function_return]));
      (void)(code_emit(C_PUNCT, 4));
      (void)(code_emit(C_PUNCT, 9));
      (void)(code_emit(C_IDENT, sym_c_symbol(name)));
      (void)(gen_array_suffixes(ty));
      (void)(code_emit(C_PUNCT, 5));
      (void)(code_emit(C_PUNCT, 6));
      (void)(gen_function_type_params(node_a[array_base]));
      (void)(code_emit(C_PUNCT, 8));
    } else {
      (void)(gen_array_base_type(ty));
      (void)(code_emit(C_IDENT, sym_c_symbol(name)));
      (void)(gen_array_suffixes(ty));
    }
  } else {
    (void)(gen_type(node_kind[ty], ty, node_value[ty]));
    (void)(code_emit(C_IDENT, sym_c_symbol(name)));
  }
}
void gen_stmt(int id) {
  gen_source_pos = node_pos[id];
  gen_source_epoch = (gen_source_epoch + 1);
  int k = node_kind[id];
  if (k == N_GLOBAL) {
    (void)(gen_alignment(node_aux[id]));
    (void)(gen_decl(node_b[id], node_a[id]));
    (void)(code_emit(C_PUNCT, 11));
    (void)(gen_initializer(node_b[id], node_c[id]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
  } else if (k == N_CONST) {
    (void)(gen_alignment(node_aux[id]));
    (void)(gen_const_decl(node_b[id], node_a[id]));
    (void)(code_emit(C_PUNCT, 11));
    (void)(gen_initializer(node_b[id], node_c[id]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
  } else if (k == N_LET) {
    int local_type = gen_substitute_type(node_b[id]);
    (void)(gen_alignment(node_aux[id]));
    (void)(gen_decl(local_type, node_a[id]));
    (void)(code_emit(C_PUNCT, 11));
    (void)(gen_initializer(local_type, node_c[id]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
  } else if (k == N_ASSIGN) {
    (void)(gen_assignment(node_a[id], node_b[id]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
  } else if (k == N_COMPOUND_ASSIGN) {
    (void)(gen_compound_assignment(node_a[id], node_value[id], node_b[id]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
  } else if ((k == N_PRINT) || (k == N_PRINTLN)) {
    (void)(code_emit(C_IDENT, (0 - 1001)));
    int pk = gen_expr_kind(node_a[id]);
    int add_newline = 0;
    if (k == N_PRINTLN)
      add_newline = 1;
    else {
    }
    if (pk == TY_STRING) {
      if (add_newline == 1)
        (void)(code_emit(C_PUNCT, 16));
      else
        (void)(code_emit(C_PUNCT, 32));
    } else if (pk == TY_CHAR) {
      if (add_newline == 1)
        (void)(code_emit(C_PUNCT, 20));
      else
        (void)(code_emit(C_PUNCT, 33));
    } else if ((pk == TY_FLOAT) || (pk == TY_DOUBLE)) {
      if (add_newline == 1)
        (void)(code_emit(C_PUNCT, 21));
      else
        (void)(code_emit(C_PUNCT, 34));
    } else if (pk == TY_LONG) {
      if (add_newline == 1)
        (void)(code_emit(C_PUNCT, 28));
      else
        (void)(code_emit(C_PUNCT, 35));
    } else if (pk == TY_LLONG) {
      if (add_newline == 1)
        (void)(code_emit(C_PUNCT, 29));
      else
        (void)(code_emit(C_PUNCT, 36));
    } else if (pk == TY_PTR) {
      if (add_newline == 1)
        (void)(code_emit(C_PUNCT, 26));
      else
        (void)(code_emit(C_PUNCT, 37));
      (void)(code_emit(C_PUNCT, 4));
      (void)(code_emit(C_KW, 4));
      (void)(code_emit(C_PUNCT, 1));
      (void)(code_emit(C_PUNCT, 5));
    } else {
      if (add_newline == 1)
        (void)(code_emit(C_PUNCT, 15));
      else
        (void)(code_emit(C_PUNCT, 38));
    }
    (void)(gen_expr(node_a[id]));
    (void)(code_emit(C_PUNCT, 8));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
  } else if (k == N_DEFER) {
    (void)(gen_defer_push(node_a[id]));
  } else if (k == N_TUPLE_BIND) {
    int binding = node_a[id];
    int index = 0;
    int tuple_ty = node_b[id];
    int rhs_temp = gen_match_temp_symbol();
    (void)(gen_type(node_kind[tuple_ty], tuple_ty, 0));
    (void)(code_emit(C_IDENT, rhs_temp));
    (void)(code_emit(C_PUNCT, 11));
    (void)(gen_expr(node_c[id]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
    while (binding != 0) {
      int elem_ty = node_a[tuple_ty];
      int i = 0;
      while ((i < index) && (elem_ty != 0)) {
        elem_ty = node_next[elem_ty];
        i = (i + 1);
      }
      if (elem_ty != 0)
        (void)(gen_type(node_kind[elem_ty], elem_ty, 0));
      else
        (void)(code_emit(C_KW, 1));
      (void)(code_emit(C_IDENT, sym_c_symbol(node_value[binding])));
      (void)(code_emit(C_PUNCT, 11));
      (void)(code_emit(C_IDENT, rhs_temp));
      (void)(code_emit(C_PUNCT, 17));
      (void)(code_emit(C_IDENT, gen_tuple_field_name(index)));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
      binding = node_next[binding];
      index = (index + 1);
    }
  } else if (k == N_MATCH) {
    (void)(code_emit(C_PUNCT, 13));
    int match_scope_start = emit_defer_count;
    (void)(gen_match_stmt(id));
    (void)(gen_emit_defer_from(match_scope_start));
    emit_defer_count = match_scope_start;
    (void)(code_emit(C_PUNCT, 14));
  } else if (k == N_EXPR) {
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_KW, 4));
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 4));
    (void)(gen_expr(node_a[id]));
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
  } else if (k == N_RETURN) {
    if ((emit_defer_count > 0) && (node_a[id] != 0)) {
      int return_type = 0;
      if (gen_active_function != 0)
        return_type = gen_substitute_type(node_b[gen_active_function]);
      else {
      }
      if (return_type != 0) {
        int return_temp = gen_match_temp_symbol();
        (void)(code_emit(C_PUNCT, 13));
        (void)(gen_type(node_kind[return_type], return_type, node_value[return_type]));
        (void)(code_emit(C_IDENT, return_temp));
        (void)(code_emit(C_PUNCT, 11));
        (void)(gen_expr(node_a[id]));
        (void)(code_emit(C_PUNCT, 12));
        (void)(code_emit(C_NEWLINE, 0));
        (void)(gen_emit_all_defers());
        (void)(code_emit(C_KW, 5));
        (void)(code_emit(C_IDENT, return_temp));
        (void)(code_emit(C_PUNCT, 12));
        (void)(code_emit(C_NEWLINE, 0));
        (void)(code_emit(C_PUNCT, 14));
      } else {
        (void)(gen_emit_all_defers());
        (void)(code_emit(C_KW, 5));
        (void)(gen_expr(node_a[id]));
        (void)(code_emit(C_PUNCT, 12));
        (void)(code_emit(C_NEWLINE, 0));
      }
    } else {
      if (emit_defer_count > 0)
        (void)(code_emit(C_PUNCT, 13));
      else {
      }
      (void)(gen_emit_all_defers());
      (void)(code_emit(C_KW, 5));
      if (node_a[id] != 0)
        (void)(gen_expr(node_a[id]));
      else {
      }
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
      if (emit_defer_count > 0)
        (void)(code_emit(C_PUNCT, 14));
      else {
      }
    }
  } else if (k == N_BREAK) {
    if (emit_loop_depth > 0) {
      (void)(code_emit(C_PUNCT, 13));
      (void)(gen_emit_defer_from(emit_loop_base[(emit_loop_depth - 1)]));
      (void)(code_emit(C_KW, 9));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
      (void)(code_emit(C_PUNCT, 14));
    } else {
      (void)(code_emit(C_KW, 9));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
    }
  } else if (k == N_CONTINUE) {
    if (emit_loop_depth > 0) {
      (void)(code_emit(C_PUNCT, 13));
      (void)(gen_emit_defer_from(emit_loop_base[(emit_loop_depth - 1)]));
      (void)(code_emit(C_KW, 10));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
      (void)(code_emit(C_PUNCT, 14));
    } else {
      (void)(code_emit(C_KW, 10));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
    }
  } else if (k == N_BLOCK) {
    (void)(code_emit(C_PUNCT, 13));
    (void)(ensure_emit_scope(emit_scope_depth));
    emit_scope_start[emit_scope_depth] = emit_defer_count;
    emit_scope_depth = (emit_scope_depth + 1);
    int item = node_a[id];
    while (item != 0) {
      (void)(gen_stmt(item));
      item = node_next[item];
    }
    emit_scope_depth = (emit_scope_depth - 1);
    (void)(gen_emit_defer_from(emit_scope_start[emit_scope_depth]));
    emit_defer_count = emit_scope_start[emit_scope_depth];
    (void)(code_emit(C_PUNCT, 14));
  } else if (k == N_IF) {
    (void)(code_emit(C_KW, 6));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_expr_condition_inner(node_a[id]));
    (void)(code_emit(C_PUNCT, 8));
    (void)(gen_stmt(node_b[id]));
    (void)(code_emit(C_KW, 7));
    (void)(gen_stmt(node_c[id]));
  } else if (k == N_FOR) {
    (void)(code_emit(C_KW, 11));
    (void)(code_emit(C_PUNCT, 4));
    (void)(gen_for_clause(node_a[id]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(gen_expr_condition_inner(node_b[id]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(gen_for_clause(node_value[id]));
    (void)(code_emit(C_PUNCT, 5));
    (void)(ensure_emit_loop(emit_loop_depth));
    emit_loop_base[emit_loop_depth] = emit_defer_count;
    emit_loop_depth = (emit_loop_depth + 1);
    int old_step = emit_for_step;
    emit_for_step = node_value[id];
    (void)(gen_stmt(node_c[id]));
    emit_for_step = old_step;
    emit_loop_depth = (emit_loop_depth - 1);
  } else if (k == N_WHILE) {
    (void)(code_emit(C_KW, 8));
    (void)(code_emit(C_PUNCT, 4));
    (void)(gen_expr_condition_inner(node_a[id]));
    (void)(code_emit(C_PUNCT, 5));
    (void)(ensure_emit_loop(emit_loop_depth));
    emit_loop_base[emit_loop_depth] = emit_defer_count;
    emit_loop_depth = (emit_loop_depth + 1);
    int old_step = emit_for_step;
    emit_for_step = node_aux[id];
    (void)(gen_stmt(node_b[id]));
    emit_for_step = old_step;
    emit_loop_depth = (emit_loop_depth - 1);
  } else {
  }
}
int gen_generic_base_equal(int formal, int actual) {
  if ((formal == 0) || (actual == 0))
    return 0;
  else {
  }
  if (node_value[formal] == node_value[actual])
    return 1;
  else {
  }
  int formal_decl = tc_find_struct_ctx(node_value[formal], node_scope[formal]);
  int actual_decl = tc_find_struct_ctx(node_value[actual], node_scope[actual]);
  if (((formal_decl != 0) && (actual_decl != 0)) && (formal_decl == actual_decl))
    return 1;
  else {
  }
  return 0;
}
void gen_unify_formal(int formal, int actual) {
  if ((formal == 0) || (actual == 0))
    return;
  else {
  }
  if (node_kind[formal] == TY_PARAM) {
    if ((node_kind[actual] == TY_PARAM) && (node_value[formal] == node_value[actual]))
      return;
    else {
    }
    if (gen_bind_find(node_value[formal]) == 0)
      (void)(gen_bind_add(node_value[formal], actual));
    else {
    }
    return;
  } else {
  }
  if ((node_kind[formal] == TY_GENERIC) && (node_kind[actual] == TY_GENERIC)) {
    if (gen_generic_base_equal(formal, actual) == 0)
      return;
    else {
    }
    int fp = node_a[formal];
    int ap = node_a[actual];
    while ((fp != 0) && (ap != 0)) {
      (void)(gen_unify_formal(fp, ap));
      fp = node_next[fp];
      ap = node_next[ap];
    }
    return;
  } else {
  }
  if ((node_kind[formal] == TY_PTR) && (node_kind[actual] == TY_PTR)) {
    (void)(gen_unify_formal(node_a[formal], node_a[actual]));
    return;
  } else {
  }
  if ((node_kind[formal] == TY_ARRAY) && (node_kind[actual] == TY_ARRAY)) {
    (void)(gen_unify_formal(node_a[formal], node_a[actual]));
    return;
  } else {
  }
  if ((node_kind[formal] == TY_DYN_ARRAY) && (node_kind[actual] == TY_DYN_ARRAY)) {
    (void)(gen_unify_formal(node_a[formal], node_a[actual]));
    return;
  } else {
  }
  if (((node_kind[formal] == TY_FUN) || (node_kind[formal] == TY_CLOSURE)) &&
      ((node_kind[actual] == TY_FUN) || (node_kind[actual] == TY_CLOSURE))) {
    int fp_fun = node_a[formal];
    int ap_fun = node_a[actual];
    while ((fp_fun != 0) && (ap_fun != 0)) {
      (void)(gen_unify_formal(fp_fun, ap_fun));
      fp_fun = node_next[fp_fun];
      ap_fun = node_next[ap_fun];
    }
    (void)(gen_unify_formal(node_b[formal], node_b[actual]));
    return;
  } else {
  }
}
void gen_bind_decl(int decl, int inst) {
  (void)(gen_bind_clear());
  if (node_kind[decl] == N_GENERIC_FUNC) {
    int p = node_c[decl];
    int a = inst;
    while ((p != 0) && (a != 0)) {
      (void)(gen_unify_formal(node_b[p], a));
      p = node_next[p];
      a = node_next[a];
    }
    return;
  } else {
  }
  int p = node_c[decl];
  int a = node_a[inst];
  while ((p != 0) && (a != 0)) {
    (void)(gen_bind_add(node_a[p], a));
    p = node_next[p];
    a = node_next[a];
  }
}
void gen_struct_decl_specialized(int decl, int inst, int cname) {
  (void)(gen_bind_decl(decl, inst));
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, cname));
  (void)(code_emit(C_PUNCT, 13));
  int f = node_a[decl];
  while (f != 0) {
    int ft = gen_substitute_type(node_b[f]);
    (void)(gen_decl(ft, node_a[f]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
    f = node_next[f];
  }
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_function_params(int params, int is_extern) {
  int p = params;
  if (p == 0)
    (void)(code_emit(C_KW, 4));
  else {
    while (p != 0) {
      if (is_extern == 1)
        (void)(gen_extern_param(gen_substitute_type(node_b[p]), node_a[p]));
      else
        (void)(gen_decl(gen_substitute_type(node_b[p]), node_a[p]));
      if (node_next[p] != 0)
        (void)(code_emit(C_PUNCT, 7));
      else {
      }
      p = node_next[p];
    }
  }
}
void gen_function_type_params(int types) {
  int p = types;
  if (p == 0)
    (void)(code_emit(C_KW, 4));
  else {
    while (p != 0) {
      int pt = gen_substitute_type(p);
      (void)(gen_type(node_kind[pt], pt, node_value[pt]));
      if (node_next[p] != 0)
        (void)(code_emit(C_PUNCT, 7));
      else {
      }
      p = node_next[p];
    }
  }
}
void gen_function_signature_parts(int ret_ty, int name, int params, int is_extern) {
  int ret = gen_substitute_type(ret_ty);
  if ((ret != 0) && (node_kind[ret] == TY_FUN)) {
    int inner_ret = gen_substitute_type(node_b[ret]);
    (void)(gen_type(node_kind[inner_ret], inner_ret, node_value[inner_ret]));
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_PUNCT, 9));
    (void)(code_emit(C_IDENT, name));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_function_params(params, is_extern));
    (void)(code_emit(C_PUNCT, 8));
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_function_type_params(node_a[ret]));
    (void)(code_emit(C_PUNCT, 8));
  } else {
    (void)(gen_type(node_kind[ret], ret, node_value[ret]));
    (void)(code_emit(C_IDENT, name));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_function_params(params, is_extern));
    (void)(code_emit(C_PUNCT, 8));
  }
}
void gen_function_specialized(int decl, int inst, int cname) {
  (void)(gen_bind_decl(decl, inst));
  (void)(gen_function_signature_parts(node_b[decl], cname, node_c[decl], 0));
  int old_active_function = gen_active_function;
  gen_active_function = decl;
  (void)(gen_stmt(node_a[decl]));
  gen_active_function = old_active_function;
  (void)(gen_bind_clear());
}
int gen_match_temp_symbol(void) {
  gen_mangle_start = (source_len + sym_text_len);
  gen_mangle_len = 0;
  (void)(gen_append_text("__basalt_match_"));
  (void)(gen_append_uint(gen_match_serial));
  gen_match_serial = (gen_match_serial + 1);
  return gen_mangle_intern(L_ID);
}
void gen_match_binding(int temp, int variant, int binding, int field) {
  (void)(gen_decl(node_b[field], node_value[binding]));
  (void)(code_emit(C_PUNCT, 11));
  (void)(code_emit(C_IDENT, temp));
  (void)(code_emit(C_PUNCT, 17));
  (void)(code_emit(C_IDENT, sym_c_symbol(node_a[variant])));
  (void)(code_emit(C_PUNCT, 17));
  (void)(code_emit(C_IDENT, sym_c_symbol(node_a[field])));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_match_stmt(int id) {
  int subject = node_a[id];
  int subject_ty = tc_emit_arg_type(subject);
  if (subject_ty == 0) {
    (void)(tc_expr(subject));
    subject_ty = tc_result_type;
  } else {
  }
  if (subject_ty == 0)
    return;
  else {
  }
  subject_ty = gen_substitute_type(subject_ty);
  int enum_decl = tc_match_enum_decl(subject_ty);
  if (enum_decl == 0)
    return;
  else {
  }
  int enum_name = node_value[enum_decl];
  int tagged = 0;
  int probe = node_a[enum_decl];
  while (probe != 0) {
    if (node_b[probe] != 0)
      tagged = 1;
    else {
    }
    probe = node_next[probe];
  }
  int temp = gen_match_temp_symbol();
  (void)(gen_type(node_kind[subject_ty], subject_ty, node_value[subject_ty]));
  (void)(code_emit(C_IDENT, temp));
  (void)(code_emit(C_PUNCT, 11));
  (void)(gen_expr(subject));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
  int arm = node_b[id];
  int branch_started = 0;
  while (arm != 0) {
    if (node_value[arm] == 0) {
      (void)(code_emit(C_KW, 7));
      (void)(code_emit(C_PUNCT, 13));
      int arm_scope_start = emit_defer_count;
      (void)(gen_stmt(node_b[arm]));
      (void)(gen_emit_defer_from(arm_scope_start));
      emit_defer_count = arm_scope_start;
      (void)(code_emit(C_PUNCT, 14));
      branch_started = 1;
    } else {
      int variant = tc_match_variant_member(enum_decl, node_value[arm]);
      if (variant != 0) {
        if (branch_started == 1)
          (void)(code_emit(C_KW, 7));
        else {
        }
        (void)(code_emit(C_KW, 6));
        (void)(code_emit(C_PUNCT, 4));
        (void)(code_emit(C_IDENT, temp));
        if (tagged == 1)
          (void)(code_emit(C_PUNCT, 17));
        else {
        }
        if (tagged == 1)
          (void)(code_emit(C_IDENT, sym_tag_id()));
        else {
        }
        (void)(code_emit(C_OP, 5));
        (void)(code_emit(C_IDENT, sym_c_symbol(sym_qualified(enum_name, node_a[variant]))));
        (void)(code_emit(C_PUNCT, 5));
        (void)(code_emit(C_PUNCT, 13));
        int field = node_b[variant];
        int binding = node_a[arm];
        while ((field != 0) && (binding != 0)) {
          (void)(gen_match_binding(temp, variant, binding, field));
          field = node_next[field];
          binding = node_next[binding];
        }
        int arm_scope_start = emit_defer_count;
        (void)(gen_stmt(node_b[arm]));
        (void)(gen_emit_defer_from(arm_scope_start));
        emit_defer_count = arm_scope_start;
        (void)(code_emit(C_PUNCT, 14));
        branch_started = 1;
      } else {
      }
    }
    arm = node_next[arm];
  }
}
void gen_tuple_decl(int ty, int name) {
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, name));
  (void)(code_emit(C_PUNCT, 13));
  int field = node_a[ty];
  int index = 0;
  while (field != 0) {
    (void)(gen_type(node_kind[field], field, node_value[field]));
    (void)(code_emit(C_IDENT, gen_tuple_field_name(index)));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
    field = node_next[field];
    index = (index + 1);
  }
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_struct_decl(int id) {
  gen_source_pos = node_pos[id];
  gen_source_epoch = (gen_source_epoch + 1);
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, sym_c_symbol(node_value[id])));
  (void)(code_emit(C_PUNCT, 13));
  int f = node_a[id];
  while (f != 0) {
    (void)(gen_decl(node_b[f], node_a[f]));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
    f = node_next[f];
  }
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_emit_complete_struct(int decl) {
  if (decl == 0)
    return;
  else {
  }
  (void)(ensure_gen_struct_state(decl));
  if (gen_struct_state[decl] == 2)
    return;
  else {
  }
  if (gen_struct_state[decl] == 1)
    return;
  else {
  }
  gen_struct_state[decl] = 1;
  int field = node_a[decl];
  while (field != 0) {
    (void)(gen_emit_complete_type(node_b[field]));
    field = node_next[field];
  }
  (void)(gen_struct_decl(decl));
  gen_struct_state[decl] = 2;
}
void gen_emit_complete_spec(int index) {
  if (index < 0)
    return;
  else {
  }
  (void)(ensure_gen_spec_state(index));
  if (gen_spec_state[index] == 2)
    return;
  else {
  }
  if (gen_spec_state[index] == 1)
    return;
  else {
  }
  gen_spec_state[index] = 1;
  int decl = gen_spec_decl[index];
  int inst = gen_spec_type[index];
  int saved_count = gen_bind_count;
  (void)(ensure_gen_bind((saved_count + saved_count)));
  int save_i = 0;
  while (save_i < saved_count) {
    gen_bind_name[(saved_count + save_i)] = gen_bind_name[save_i];
    gen_bind_type[(saved_count + save_i)] = gen_bind_type[save_i];
    save_i = (save_i + 1);
  }
  (void)(gen_bind_decl(decl, inst));
  int field = node_a[decl];
  while (field != 0) {
    (void)(gen_emit_complete_type(gen_substitute_type(node_b[field])));
    field = node_next[field];
  }
  (void)(gen_bind_clear());
  int restore_i = 0;
  while (restore_i < saved_count) {
    gen_bind_name[restore_i] = gen_bind_name[(saved_count + restore_i)];
    gen_bind_type[restore_i] = gen_bind_type[(saved_count + restore_i)];
    restore_i = (restore_i + 1);
  }
  gen_bind_count = saved_count;
  (void)(gen_struct_decl_specialized(decl, inst, gen_spec_name[index]));
  (void)(gen_bind_clear());
  int restore_j = 0;
  while (restore_j < saved_count) {
    gen_bind_name[restore_j] = gen_bind_name[(saved_count + restore_j)];
    gen_bind_type[restore_j] = gen_bind_type[(saved_count + restore_j)];
    restore_j = (restore_j + 1);
  }
  gen_bind_count = saved_count;
  gen_spec_state[index] = 2;
}
void gen_emit_complete_type(int ty) {
  int q = gen_substitute_type(ty);
  if (q == 0)
    return;
  else {
  }
  if (node_kind[q] == TY_PTR)
    return;
  else {
  }
  if (node_kind[q] == TY_ARRAY) {
    (void)(gen_emit_complete_type(node_a[q]));
    return;
  } else {
  }
  if (node_kind[q] == TY_DYN_ARRAY)
    return;
  else {
  }
  if (node_kind[q] == TY_NAMED) {
    (void)(gen_emit_complete_struct(tc_find_struct(node_value[q])));
    return;
  } else {
  }
  if (node_kind[q] == TY_GENERIC) {
    int decl = tc_find_struct(node_value[q]);
    int index = gen_find_spec_index(decl, gen_mangled_type_symbol(q));
    if (index > (0 - 1))
      (void)(gen_emit_complete_spec(index));
    else {
    }
  } else {
  }
}
void gen_tagged_enum_decl(int id) {
  int enum_name = node_value[id];
  int c_enum_name = sym_c_symbol(enum_name);
  int tag_name = sym_c_symbol(sym_qualified(enum_name, sym_tag_id()));
  (void)(code_emit(C_KW, 14));
  (void)(code_emit(C_KW, 13));
  (void)(code_emit(C_IDENT, tag_name));
  (void)(code_emit(C_PUNCT, 13));
  int f = node_a[id];
  while (f != 0) {
    (void)(code_emit(C_IDENT, sym_c_symbol(sym_qualified(enum_name, node_a[f]))));
    (void)(code_emit(C_PUNCT, 11));
    (void)(code_emit(C_INT, node_value[f]));
    if (node_next[f] != 0)
      (void)(code_emit(C_PUNCT, 7));
    else {
    }
    (void)(code_emit(C_NEWLINE, 0));
    f = node_next[f];
  }
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_IDENT, tag_name));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
  (void)(code_emit(C_KW, 14));
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, c_enum_name));
  (void)(code_emit(C_PUNCT, 13));
  (void)(code_emit(C_IDENT, tag_name));
  (void)(code_emit(C_PUNCT, 18));
  (void)(code_emit(C_IDENT, sym_tag_id()));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_KW, 30));
  (void)(code_emit(C_PUNCT, 13));
  f = node_a[id];
  while (f != 0) {
    if (node_b[f] != 0) {
      (void)(code_emit(C_KW, 12));
      (void)(code_emit(C_PUNCT, 13));
      int field = node_b[f];
      while (field != 0) {
        (void)(gen_decl(node_b[field], node_a[field]));
        (void)(code_emit(C_PUNCT, 12));
        (void)(code_emit(C_NEWLINE, 0));
        field = node_next[field];
      }
      (void)(code_emit(C_PUNCT, 14));
      (void)(code_emit(C_IDENT, sym_c_symbol(node_a[f])));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
    } else {
    }
    f = node_next[f];
  }
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_IDENT, c_enum_name));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_enum_decl(int id) {
  gen_source_pos = node_pos[id];
  gen_source_epoch = (gen_source_epoch + 1);
  int tagged = 0;
  int probe = node_a[id];
  while (probe != 0) {
    if (node_b[probe] != 0)
      tagged = 1;
    else {
    }
    probe = node_next[probe];
  }
  if (tagged == 1) {
    (void)(gen_tagged_enum_decl(id));
    return;
  } else {
  }
  (void)(code_emit(C_KW, 14));
  (void)(code_emit(C_KW, 13));
  (void)(code_emit(C_IDENT, sym_c_symbol(node_value[id])));
  (void)(code_emit(C_PUNCT, 13));
  int f = node_a[id];
  while (f != 0) {
    (void)(code_emit(C_IDENT, sym_c_symbol(sym_qualified(node_value[id], node_a[f]))));
    (void)(code_emit(C_PUNCT, 11));
    (void)(code_emit(C_INT, node_value[f]));
    if (node_next[f] != 0)
      (void)(code_emit(C_PUNCT, 7));
    else {
    }
    (void)(code_emit(C_NEWLINE, 0));
    f = node_next[f];
  }
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_IDENT, sym_c_symbol(node_value[id])));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_extern_param(int ty, int name) {
  if (node_kind[ty] == TY_STRING) {
    (void)(code_emit(C_KW, 16));
    (void)(code_emit(C_KW, 3));
    (void)(code_emit(C_IDENT, sym_c_symbol(name)));
  } else
    (void)(gen_decl(ty, name));
}
void gen_function_signature(int id) {
  int ret = gen_substitute_type(node_b[id]);
  if (bi_has_flag(node_value[id], BI_FLAG_MAIN) == 1) {
    (void)(code_emit(C_KW, 1));
    (void)(code_emit(C_IDENT, sym_c_symbol(node_value[id])));
    (void)(code_emit(C_PUNCT, 6));
    (void)(gen_function_params(node_c[id], 0));
    (void)(code_emit(C_PUNCT, 8));
  } else
    (void)(gen_function_signature_parts(ret, sym_c_symbol(node_value[id]), node_c[id],
                                        (node_kind[id] == N_EXTERN)));
}
void gen_prototype(int id) {
  gen_source_pos = node_pos[id];
  gen_source_epoch = (gen_source_epoch + 1);
  (void)(gen_function_signature(id));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_function(int id) {
  gen_source_pos = node_pos[id];
  gen_source_epoch = (gen_source_epoch + 1);
  int old_active_function = gen_active_function;
  gen_active_function = id;
  (void)(gen_function_signature(id));
  if ((bi_has_flag(node_value[id], BI_FLAG_MAIN) == 1) && (node_kind[node_a[id]] == N_BLOCK)) {
    (void)(code_emit(C_PUNCT, 13));
    (void)(ensure_emit_scope(emit_scope_depth));
    emit_scope_start[emit_scope_depth] = emit_defer_count;
    emit_scope_depth = (emit_scope_depth + 1);
    int item = node_a[node_a[id]];
    while (item != 0) {
      (void)(gen_stmt(item));
      item = node_next[item];
    }
    emit_scope_depth = (emit_scope_depth - 1);
    (void)(gen_emit_defer_from(emit_scope_start[emit_scope_depth]));
    emit_defer_count = emit_scope_start[emit_scope_depth];
    (void)(code_emit(C_KW, 5));
    (void)(code_emit(C_INT, 0));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
    (void)(code_emit(C_PUNCT, 14));
  } else
    (void)(gen_stmt(node_a[id]));
  gen_active_function = old_active_function;
}
void gen_closure_emit_env(int serial, int closure_id) {
  int env_name_emit = gen_closure_env_name(serial);
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, env_name_emit));
  (void)(code_emit(C_PUNCT, 13));
  int cap_emit = node_a[closure_id];
  if (cap_emit == 0) {
    (void)(code_emit(C_KW, 17));
    (void)(code_emit(C_IDENT, gen_closure_name(serial, "__basalt_unused_")));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
  } else {
  }
  while (cap_emit != 0) {
    int source_ty_emit = node_b[cap_emit];
    if ((node_aux[cap_emit] == 2) || (node_aux[cap_emit] == 3)) {
      int ref_ty_emit = ast_node(TY_PTR, source_ty_emit, 0, 0, 0, 0);
      (void)(gen_type(TY_PTR, ref_ty_emit, 0));
    } else
      (void)(gen_type(node_kind[source_ty_emit], source_ty_emit, node_value[source_ty_emit]));
    (void)(code_emit(C_IDENT, sym_c_symbol(node_a[cap_emit])));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
    cap_emit = node_next[cap_emit];
  }
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_closure_emit_value(int closure_id) {
  int sig_emit = gen_closure_signature(closure_id);
  int value_name_emit = gen_mangled_type_symbol(sig_emit);
  int duplicate_value_emit = 0;
  int value_scan_emit = 0;
  while (value_scan_emit < gen_closure_count) {
    if (gen_closure_node[value_scan_emit] == closure_id) {
      break;
    } else {
    }
    if (gen_closure_value_type_name[value_scan_emit] == value_name_emit)
      duplicate_value_emit = 1;
    else {
    }
    value_scan_emit = (value_scan_emit + 1);
  }
  if (duplicate_value_emit == 1)
    return;
  else {
  }
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, value_name_emit));
  (void)(code_emit(C_PUNCT, 13));
  (void)(code_emit(C_KW, 4));
  (void)(code_emit(C_PUNCT, 1));
  (void)(code_emit(C_IDENT, gen_closure_name(0, "env")));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
  int ret_emit = node_b[sig_emit];
  (void)(gen_type(node_kind[ret_emit], ret_emit, node_value[ret_emit]));
  (void)(code_emit(C_PUNCT, 4));
  (void)(code_emit(C_PUNCT, 9));
  (void)(code_emit(C_IDENT, gen_closure_name(0, "fn")));
  (void)(code_emit(C_PUNCT, 5));
  (void)(code_emit(C_PUNCT, 4));
  (void)(code_emit(C_KW, 4));
  (void)(code_emit(C_PUNCT, 1));
  int param_emit = node_a[sig_emit];
  if (param_emit != 0)
    (void)(code_emit(C_PUNCT, 7));
  else {
  }
  while (param_emit != 0) {
    (void)(gen_type(node_kind[param_emit], param_emit, node_value[param_emit]));
    if (node_next[param_emit] != 0)
      (void)(code_emit(C_PUNCT, 7));
    else {
    }
    param_emit = node_next[param_emit];
  }
  (void)(code_emit(C_PUNCT, 8));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_closure_emit_invoke_prototype(int serial, int closure_id) {
  int sig_proto = gen_closure_signature(closure_id);
  int ret_proto = node_b[sig_proto];
  (void)(gen_type(node_kind[ret_proto], ret_proto, node_value[ret_proto]));
  (void)(code_emit(C_IDENT, gen_closure_invoke_name(serial)));
  (void)(code_emit(C_PUNCT, 6));
  (void)(code_emit(C_KW, 4));
  (void)(code_emit(C_PUNCT, 1));
  (void)(code_emit(C_IDENT, gen_closure_env_param_name(serial)));
  int param_proto = node_c[closure_id];
  if (param_proto != 0)
    (void)(code_emit(C_PUNCT, 7));
  else {
  }
  while (param_proto != 0) {
    (void)(gen_decl(node_b[param_proto], node_a[param_proto]));
    if (node_next[param_proto] != 0)
      (void)(code_emit(C_PUNCT, 7));
    else {
    }
    param_proto = node_next[param_proto];
  }
  (void)(code_emit(C_PUNCT, 8));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_closure_emit_call_helper(int sig) {
  int call_name_emit = gen_closure_call_name(sig);
  int ret_call = node_b[sig];
  (void)(gen_type(node_kind[ret_call], ret_call, node_value[ret_call]));
  (void)(code_emit(C_IDENT, call_name_emit));
  (void)(code_emit(C_PUNCT, 6));
  (void)(gen_type(TY_CLOSURE, sig, 0));
  (void)(code_emit(C_IDENT, gen_closure_name(0, "value")));
  int param_call = node_a[sig];
  if (param_call != 0)
    (void)(code_emit(C_PUNCT, 7));
  else {
  }
  while (param_call != 0) {
    (void)(gen_decl(param_call, gen_closure_name(0, "arg")));
    if (node_next[param_call] != 0)
      (void)(code_emit(C_PUNCT, 7));
    else {
    }
    param_call = node_next[param_call];
  }
  (void)(code_emit(C_PUNCT, 8));
  (void)(code_emit(C_PUNCT, 13));
  if (node_kind[ret_call] == TY_VOID) {
    (void)(code_emit(C_IDENT, gen_closure_name(0, "value")));
    (void)(code_emit(C_PUNCT, 17));
    (void)(code_emit(C_IDENT, gen_closure_name(0, "fn")));
    (void)(code_emit(C_PUNCT, 6));
  } else {
    (void)(code_emit(C_KW, 5));
    (void)(code_emit(C_IDENT, gen_closure_name(0, "value")));
    (void)(code_emit(C_PUNCT, 17));
    (void)(code_emit(C_IDENT, gen_closure_name(0, "fn")));
    (void)(code_emit(C_PUNCT, 6));
  }
  (void)(code_emit(C_IDENT, gen_closure_name(0, "value")));
  (void)(code_emit(C_PUNCT, 17));
  (void)(code_emit(C_IDENT, gen_closure_name(0, "env")));
  int arg_call = node_a[sig];
  if (arg_call != 0)
    (void)(code_emit(C_PUNCT, 7));
  else {
  }
  while (arg_call != 0) {
    (void)(code_emit(C_IDENT, gen_closure_name(0, "arg")));
    if (node_next[arg_call] != 0)
      (void)(code_emit(C_PUNCT, 7));
    else {
    }
    arg_call = node_next[arg_call];
  }
  (void)(code_emit(C_PUNCT, 8));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_closure_emit_factory(int serial, int closure_id) {
  int sig_factory = gen_closure_signature(closure_id);
  int value_factory = gen_mangled_type_symbol(sig_factory);
  (void)(gen_type(TY_CLOSURE, sig_factory, 0));
  (void)(code_emit(C_IDENT, gen_closure_factory_name(serial)));
  (void)(code_emit(C_PUNCT, 6));
  int cap_factory = node_a[closure_id];
  if (cap_factory == 0)
    (void)(code_emit(C_KW, 4));
  else {
    while (cap_factory != 0) {
      int cap_ty_factory = node_b[cap_factory];
      if ((node_aux[cap_factory] == 2) || (node_aux[cap_factory] == 3)) {
        int cap_ptr_factory = ast_node(TY_PTR, cap_ty_factory, 0, 0, 0, 0);
        (void)(gen_decl(cap_ptr_factory, node_a[cap_factory]));
      } else
        (void)(gen_decl(cap_ty_factory, node_a[cap_factory]));
      if (node_next[cap_factory] != 0)
        (void)(code_emit(C_PUNCT, 7));
      else {
      }
      cap_factory = node_next[cap_factory];
    }
  }
  (void)(code_emit(C_PUNCT, 8));
  (void)(code_emit(C_PUNCT, 13));
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, gen_closure_env_name(serial)));
  (void)(code_emit(C_PUNCT, 1));
  (void)(code_emit(C_IDENT, gen_closure_env_local_name(serial)));
  (void)(code_emit(C_PUNCT, 11));
  (void)(code_emit(C_IDENT, (0 - 1016)));
  (void)(code_emit(C_PUNCT, 6));
  (void)(code_emit(C_INT, 1));
  (void)(code_emit(C_PUNCT, 7));
  (void)(code_emit(C_IDENT, (0 - 1011)));
  (void)(code_emit(C_PUNCT, 4));
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, gen_closure_env_name(serial)));
  (void)(code_emit(C_PUNCT, 5));
  (void)(code_emit(C_PUNCT, 8));
  (void)(code_emit(C_PUNCT, 12));
  cap_factory = node_a[closure_id];
  while (cap_factory != 0) {
    (void)(code_emit(C_IDENT, gen_closure_env_local_name(serial)));
    (void)(code_emit(C_PUNCT, 27));
    (void)(code_emit(C_IDENT, sym_c_symbol(node_a[cap_factory])));
    (void)(code_emit(C_PUNCT, 11));
    (void)(code_emit(C_IDENT, sym_c_symbol(node_a[cap_factory])));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
    cap_factory = node_next[cap_factory];
  }
  (void)(code_emit(C_KW, 5));
  (void)(code_emit(C_PUNCT, 4));
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, value_factory));
  (void)(code_emit(C_PUNCT, 5));
  (void)(code_emit(C_PUNCT, 24));
  (void)(code_emit(C_PUNCT, 17));
  (void)(code_emit(C_IDENT, gen_closure_name(0, "env")));
  (void)(code_emit(C_PUNCT, 11));
  (void)(code_emit(C_IDENT, gen_closure_env_local_name(serial)));
  (void)(code_emit(C_PUNCT, 7));
  (void)(code_emit(C_PUNCT, 17));
  (void)(code_emit(C_IDENT, gen_closure_name(0, "fn")));
  (void)(code_emit(C_PUNCT, 11));
  (void)(code_emit(C_IDENT, gen_closure_invoke_name(serial)));
  (void)(code_emit(C_PUNCT, 25));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_PUNCT, 14));
  (void)(code_emit(C_NEWLINE, 0));
}
void gen_closure_emit_invoke(int serial, int closure_id) {
  int sig_invoke = gen_closure_signature(closure_id);
  int ret_invoke = node_b[sig_invoke];
  (void)(gen_type(node_kind[ret_invoke], ret_invoke, node_value[ret_invoke]));
  (void)(code_emit(C_IDENT, gen_closure_invoke_name(serial)));
  (void)(code_emit(C_PUNCT, 6));
  (void)(code_emit(C_KW, 4));
  (void)(code_emit(C_PUNCT, 1));
  (void)(code_emit(C_IDENT, gen_closure_env_param_name(serial)));
  int param_invoke = node_c[closure_id];
  if (param_invoke != 0)
    (void)(code_emit(C_PUNCT, 7));
  else {
  }
  while (param_invoke != 0) {
    (void)(gen_decl(node_b[param_invoke], node_a[param_invoke]));
    if (node_next[param_invoke] != 0)
      (void)(code_emit(C_PUNCT, 7));
    else {
    }
    param_invoke = node_next[param_invoke];
  }
  (void)(code_emit(C_PUNCT, 8));
  (void)(code_emit(C_PUNCT, 13));
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, gen_closure_env_name(serial)));
  (void)(code_emit(C_PUNCT, 1));
  (void)(code_emit(C_IDENT, gen_closure_env_local_name(serial)));
  (void)(code_emit(C_PUNCT, 11));
  (void)(code_emit(C_PUNCT, 4));
  (void)(code_emit(C_KW, 12));
  (void)(code_emit(C_IDENT, gen_closure_env_name(serial)));
  (void)(code_emit(C_PUNCT, 1));
  (void)(code_emit(C_PUNCT, 5));
  (void)(code_emit(C_IDENT, gen_closure_env_param_name(serial)));
  (void)(code_emit(C_PUNCT, 12));
  (void)(code_emit(C_NEWLINE, 0));
  int cap_invoke = node_a[closure_id];
  if (cap_invoke == 0) {
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_KW, 4));
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 4));
    (void)(code_emit(C_IDENT, gen_closure_env_local_name(serial)));
    (void)(code_emit(C_PUNCT, 5));
    (void)(code_emit(C_PUNCT, 12));
    (void)(code_emit(C_NEWLINE, 0));
  } else {
  }
  while (cap_invoke != 0) {
    if (node_aux[cap_invoke] == 1) {
      (void)(gen_type(node_kind[node_b[cap_invoke]], node_b[cap_invoke],
                      node_value[node_b[cap_invoke]]));
      (void)(code_emit(C_IDENT, sym_c_symbol(node_a[cap_invoke])));
      (void)(code_emit(C_PUNCT, 11));
      (void)(code_emit(C_IDENT, gen_closure_env_local_name(serial)));
      (void)(code_emit(C_PUNCT, 27));
      (void)(code_emit(C_IDENT, sym_c_symbol(node_a[cap_invoke])));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
    } else {
    }
    cap_invoke = node_next[cap_invoke];
  }
  int old_active_closure_emit = gen_active_closure;
  int old_active_function_emit = gen_active_function;
  gen_active_closure = closure_id;
  gen_active_function = closure_id;
  (void)(gen_stmt(node_b[closure_id]));
  (void)(code_emit(C_PUNCT, 14));
  gen_active_function = old_active_function_emit;
  gen_active_closure = old_active_closure_emit;
}
void gen_closure_emit_all(void) {
  int closure_i = 0;
  while (closure_i < gen_closure_count) {
    (void)(gen_closure_emit_value(gen_closure_node[closure_i]));
    closure_i = (closure_i + 1);
  }
  closure_i = 0;
  while (closure_i < gen_closure_count) {
    (void)(gen_closure_emit_env(gen_closure_serial[closure_i], gen_closure_node[closure_i]));
    closure_i = (closure_i + 1);
  }
  closure_i = 0;
  while (closure_i < gen_closure_count) {
    (void)(gen_closure_emit_invoke_prototype(gen_closure_serial[closure_i],
                                             gen_closure_node[closure_i]));
    closure_i = (closure_i + 1);
  }
  closure_i = 0;
  while (closure_i < gen_closure_count) {
    int helper_seen = 0;
    int helper_scan = 0;
    while (helper_scan < closure_i) {
      if (gen_closure_value_type_name[helper_scan] == gen_closure_value_type_name[closure_i])
        helper_seen = 1;
      else {
      }
      helper_scan = (helper_scan + 1);
    }
    if (helper_seen == 0)
      (void)(gen_closure_emit_call_helper(gen_closure_sig[closure_i]));
    else {
    }
    closure_i = (closure_i + 1);
  }
  closure_i = 0;
  while (closure_i < gen_closure_count) {
    (void)(gen_closure_emit_factory(gen_closure_serial[closure_i], gen_closure_node[closure_i]));
    closure_i = (closure_i + 1);
  }
  closure_i = 0;
  while (closure_i < gen_closure_count) {
    (void)(gen_closure_emit_invoke(gen_closure_serial[closure_i], gen_closure_node[closure_i]));
    closure_i = (closure_i + 1);
  }
}
void gen_program(int id) {
  int gen_saved_node_count = node_count;
  gen_source_pos = 0;
  gen_source_epoch = 0;
  gen_match_serial = 0;
  gen_closure_count = 0;
  gen_closure_serial_next = 0;
  gen_tuple_count = 0;
  ffi_header_count = 0;
  (void)(code_reset());
  gen_spec_count = 0;
  gen_name_override = 0;
  emit_defer_count = 0;
  emit_scope_depth = 0;
  emit_loop_depth = 0;
  int item = node_a[id];
  while (item != 0) {
    if (node_kind[item] == N_EXTERN) {
      if (node_a[item] != 0)
        (void)(ffi_header_register(node_a[item]));
      else {
      }
    } else {
    }
    if ((node_kind[item] == N_GLOBAL) || (node_kind[item] == N_CONST))
      (void)(gen_collect_stmt(item));
    else if (node_kind[item] == N_STRUCT) {
      int sf = node_a[item];
      while (sf != 0) {
        (void)(gen_collect_type(node_b[sf]));
        sf = node_next[sf];
      }
    } else if (node_kind[item] == N_FUNC) {
      (void)(gen_collect_type(node_b[item]));
      int pp = node_c[item];
      while (pp != 0) {
        (void)(gen_collect_type(node_b[pp]));
        pp = node_next[pp];
      }
      (void)(gen_collect_stmt(node_a[item]));
    } else {
    }
    item = node_next[item];
  }
  int tuple_i = 0;
  while (tuple_i < gen_tuple_count) {
    (void)(gen_tuple_decl(gen_tuple_type[tuple_i], gen_tuple_name[tuple_i]));
    tuple_i = (tuple_i + 1);
  }
  int scan_si = 0;
  while (scan_si < gen_spec_count) {
    if (gen_spec_kind[scan_si] == 2) {
      int scan_decl = gen_spec_decl[scan_si];
      int old_active_scan = gen_active_function;
      (void)(gen_bind_decl(scan_decl, gen_spec_type[scan_si]));
      gen_active_function = scan_decl;
      (void)(gen_collect_stmt(node_a[scan_decl]));
      gen_active_function = old_active_scan;
      (void)(gen_bind_clear());
    } else {
    }
    scan_si = (scan_si + 1);
  }
  item = node_a[id];
  while (item != 0) {
    if (node_kind[item] == N_STRUCT) {
      (void)(code_emit(C_KW, 14));
      (void)(code_emit(C_KW, 12));
      (void)(code_emit(C_IDENT, sym_c_symbol(node_value[item])));
      (void)(code_emit(C_PUNCT, 22));
      (void)(code_emit(C_IDENT, sym_c_symbol(node_value[item])));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
    } else {
    }
    item = node_next[item];
  }
  int si = 0;
  while (si < gen_spec_count) {
    if (gen_spec_kind[si] == 1) {
      (void)(code_emit(C_KW, 14));
      (void)(code_emit(C_KW, 12));
      (void)(code_emit(C_IDENT, gen_spec_name[si]));
      (void)(code_emit(C_PUNCT, 22));
      (void)(code_emit(C_IDENT, gen_spec_name[si]));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
    } else {
    }
    si = (si + 1);
  }
  (void)(gen_closure_emit_all());
  (void)(ensure_gen_struct_state((node_count + 1)));
  (void)(ensure_gen_spec_state(gen_spec_count));
  int reset_struct = 0;
  while (reset_struct < gen_struct_state_cap) {
    gen_struct_state[reset_struct] = 0;
    reset_struct = (reset_struct + 1);
  }
  int reset_spec = 0;
  while (reset_spec < gen_spec_count) {
    gen_spec_state[reset_spec] = 0;
    reset_spec = (reset_spec + 1);
  }
  item = node_a[id];
  while (item != 0) {
    if (node_kind[item] == N_STRUCT)
      (void)(gen_emit_complete_struct(item));
    else if (node_kind[item] == N_ENUM)
      (void)(gen_enum_decl(item));
    else {
    }
    item = node_next[item];
  }
  si = 0;
  while (si < gen_spec_count) {
    if (gen_spec_kind[si] == 1)
      (void)(gen_emit_complete_spec(si));
    else {
    }
    si = (si + 1);
  }
  item = node_a[id];
  while (item != 0) {
    if ((node_kind[item] == N_FUNC) || (node_kind[item] == N_EXTERN))
      (void)(gen_prototype(item));
    else {
    }
    item = node_next[item];
  }
  si = 0;
  while (si < gen_spec_count) {
    if (gen_spec_kind[si] == 2) {
      (void)(gen_bind_decl(gen_spec_decl[si], gen_spec_type[si]));
      (void)(gen_function_signature_parts(node_b[gen_spec_decl[si]], gen_spec_name[si],
                                          node_c[gen_spec_decl[si]], 0));
      (void)(code_emit(C_PUNCT, 12));
      (void)(code_emit(C_NEWLINE, 0));
      (void)(gen_bind_clear());
    } else {
    }
    si = (si + 1);
  }
  item = node_a[id];
  while (item != 0) {
    if ((node_kind[item] == N_GLOBAL) || (node_kind[item] == N_CONST))
      (void)(gen_stmt(item));
    else {
    }
    item = node_next[item];
  }
  si = 0;
  while (si < gen_spec_count) {
    if (gen_spec_kind[si] == 2)
      (void)(gen_function_specialized(gen_spec_decl[si], gen_spec_type[si], gen_spec_name[si]));
    else {
    }
    si = (si + 1);
  }
  item = node_a[id];
  while (item != 0) {
    if (node_kind[item] == N_FUNC)
      (void)(gen_function(item));
    else {
    }
    item = node_next[item];
  }
  node_count = gen_saved_node_count;
}
int build_regression_ast(void) {
  node_count = 1;
  payload_count = 1;
  int two = ast_node(N_INT, 0, 0, 0, 2, 0);
  int three = ast_node(N_INT, 0, 0, 0, 3, 0);
  int sum = ast_node(N_BINOP, two, three, 0, OP_ADD, 0);
  int print_stmt = ast_node(N_PRINT, sum, 0, 0, 0, 0);
  int zero = ast_node(N_INT, 0, 0, 0, 0, 0);
  int ret = ast_node(N_RETURN, zero, 0, 0, 0, 0);
  int body_items = ast_link(print_stmt, ret);
  int body = ast_node(N_BLOCK, body_items, 0, 0, 0, 0);
  int main_fn = ast_node(N_FUNC, body, 0, 0, 1, TY_INT);
  return ast_node(N_PROGRAM, main_fn, 0, 0, 0, 0);
}
void generator_regression_main(void) {
  int program = build_regression_ast();
  (void)(gen_program(program));
  int first_count = code_count;
  (void)(ensure_snapshot(first_count));
  int i = 0;
  while (i < first_count) {
    snapshot_kind[i] = code_kind[i];
    snapshot_value[i] = code_value[i];
    i = (i + 1);
  }
  (void)(gen_program(program));
  int same = 1;
  i = 0;
  if (code_count != first_count)
    same = 0;
  else {
  }
  while (i < first_count) {
    if (code_kind[i] != snapshot_kind[i])
      same = 0;
    else {
    }
    if (code_value[i] != snapshot_value[i])
      same = 0;
    else {
    }
    i = (i + 1);
  }
  if (same == 1) {
    (void)(runtime_write_string("generator_regression: PASS"));
    (void)(runtime_write_char(10));
  } else {
    (void)(runtime_write_string("generator_regression: FAIL"));
    (void)(runtime_write_char(10));
  }
}
void input_reset(void) {
  input_count = 0;
  input_pos = 0;
}
void input_put(int kind, int value, int text, int pos) {
  (void)(ensure_input(input_count));
  input_kind[input_count] = kind;
  input_value[input_count] = value;
  input_text[input_count] = text;
  input_source_pos[input_count] = pos;
  input_count = (input_count + 1);
}
int input_peek(void) {
  if (input_pos < input_count)
    return input_kind[input_pos];
  else
    return T_EOF;
}
int input_payload(void) {
  if (input_pos < input_count)
    return input_value[input_pos];
  else
    return 0;
}
int input_text_payload(void) {
  if (input_pos < input_count)
    return input_text[input_pos];
  else
    return 0;
}
int input_take(int kind) {
  if (input_peek() == kind) {
    input_pos = (input_pos + 1);
    return 1;
  } else
    return 0;
}
int ast_generic_param(int name) {
  int p = ast_generic_scope;
  while (p != 0) {
    if (node_a[p] == name)
      return 1;
    else {
    }
    p = node_next[p];
  }
  return 0;
}
int ast_generic_params(void) {
  int params = 0;
  if (input_take(T_LT) == 0)
    return 0;
  else {
  }
  while (1 == 1) {
    if (input_peek() != T_ID)
      return (0 - 1);
    else {
    }
    int name = input_payload();
    input_pos = (input_pos + 1);
    int p = ast_node(N_PARAM, name, 0, 0, 0, 0);
    if (params == 0)
      params = p;
    else
      params = ast_link(params, p);
    if (input_take(T_COMMA) == 1) {
    } else {
      break;
    }
  }
  if (input_take(T_GT) == 0)
    return (0 - 1);
  else {
  }
  return params;
}
int ast_append_array_dimension(int ty, int size) {
  if ((ty == 0) || (node_kind[ty] != TY_ARRAY))
    return ast_node(TY_ARRAY, ty, 0, 0, size, 0);
  else {
  }
  int child = ast_append_array_dimension(node_a[ty], size);
  return ast_node(TY_ARRAY, child, 0, 0, node_value[ty], 0);
}
void ensure_ast_consts(int need) {
  if (need < ast_const_cap)
    return;
  else {
  }
  int n = next_capacity(ast_const_cap, need);
  ast_const_name = grow_ints(ast_const_name, ast_const_cap, n);
  ast_const_value = grow_ints(ast_const_value, ast_const_cap, n);
  ast_const_cap = n;
}
void ast_const_add(int name, int value) {
  (void)(ensure_ast_consts(ast_const_count));
  ast_const_name[ast_const_count] = name;
  ast_const_value[ast_const_count] = value;
  ast_const_count = (ast_const_count + 1);
}
int ast_const_find(int name) {
  int i = (ast_const_count - 1);
  while (i >= 0) {
    if (ast_const_name[i] == name)
      return (i + 1);
    else {
    }
    i = (i - 1);
  }
  return 0;
}
int ast_const_add_checked(int left, int right) {
  if (right > (2147483647 - left))
    return (0 - 1);
  else {
  }
  return (left + right);
}
int ast_const_sub_checked(int left, int right) {
  if (right > left)
    return (0 - 1);
  else {
  }
  return (left - right);
}
int ast_const_mul_checked(int left, int right) {
  if ((left != 0) && (right > (2147483647 / left)))
    return (0 - 1);
  else {
  }
  return (left * right);
}
int ast_const_eval(int id) {
  if (id == 0)
    return (0 - 1);
  else {
  }
  if (node_kind[id] == N_INT) {
    if (node_value[id] < 0)
      return (0 - 1);
    else {
    }
    return node_value[id];
  } else {
  }
  if (node_kind[id] == N_VAR) {
    int found = ast_const_find(node_value[id]);
    if ((found == 0) && (ast_namespace_scope != 0))
      found = ast_const_find(sym_qualified(ast_namespace_scope, node_value[id]));
    else {
    }
    if (found != 0)
      return ast_const_value[(found - 1)];
    else {
    }
    return (0 - 1);
  } else {
  }
  if (node_kind[id] != N_BINOP)
    return (0 - 1);
  else {
  }
  int left = ast_const_eval(node_a[id]);
  int right = ast_const_eval(node_b[id]);
  if ((left < 0) || (right < 0))
    return (0 - 1);
  else {
  }
  if (node_value[id] == OP_ADD)
    return ast_const_add_checked(left, right);
  else {
  }
  if (node_value[id] == OP_SUB)
    return ast_const_sub_checked(left, right);
  else {
  }
  if (node_value[id] == OP_MUL)
    return ast_const_mul_checked(left, right);
  else {
  }
  if (node_value[id] == OP_DIV) {
    if (right == 0)
      return (0 - 1);
    else {
    }
    return (left / right);
  } else {
  }
  if (node_value[id] == OP_MOD) {
    if (right == 0)
      return (0 - 1);
    else {
    }
    return (left % right);
  } else {
  }
  return (0 - 1);
}
int ast_array_dimension_value(void) {
  if (input_peek() == T_INT) {
    int raw_size = input_payload();
    input_pos = (input_pos + 1);
    return raw_size;
  } else {
  }
  if ((input_peek() != T_ID) && (input_peek() != T_ARRAY))
    return (0 - 1);
  else {
  }
  int name = input_payload();
  input_pos = (input_pos + 1);
  int qualified = 0;
  while (input_take(T_SCOPE) == 1) {
    if ((input_peek() != T_ID) && (input_peek() != T_ARRAY))
      return (0 - 1);
    else {
    }
    int rhs = input_payload();
    input_pos = (input_pos + 1);
    name = sym_qualified(name, rhs);
    qualified = 1;
  }
  if ((ast_namespace_scope != 0) && (qualified == 0))
    name = sym_qualified(ast_namespace_scope, name);
  else {
  }
  int found = ast_const_find(name);
  if (found == 0)
    return (0 - 1);
  else {
  }
  return ast_const_value[(found - 1)];
}
int ast_type(void) {
  int base = 0;
  int named = 0;
  int ty = 0;
  if (input_take(T_LPAREN) == 1) {
    int first = ast_type();
    if (first == 0)
      return 0;
    else {
    }
    if (input_take(T_COMMA) == 1) {
      int items = first;
      while (1 == 1) {
        int item = ast_type();
        if (item == 0)
          return 0;
        else {
        }
        items = ast_link(items, item);
        if (input_take(T_COMMA) == 0) {
          {
            break;
          }
        } else {
        }
      }
      if (input_take(T_RPAREN) == 0)
        return 0;
      else {
      }
      ty = ast_node(TY_TUPLE, items, 0, 0, 0, 0);
    } else {
      if (input_take(T_RPAREN) == 0)
        return 0;
      else {
      }
      ty = first;
    }
    while (input_take(T_STAR) == 1) {
      ty = ast_node(TY_PTR, ty, 0, 0, 0, 0);
    }
    while (input_take(T_LBRACK) == 1) {
      int size = ast_array_dimension_value();
      if (size < 0)
        return 0;
      else {
      }
      if (input_take(T_RBRACK) == 0)
        return 0;
      else {
      }
      ty = ast_append_array_dimension(ty, size);
    }
    return ty;
  } else {
  }
  if (input_take(T_CLOSURE) == 1) {
    if (input_take(T_LPAREN) == 0)
      return 0;
    else {
    }
    int args = 0;
    if (input_peek() != T_RPAREN) {
      int at = ast_type();
      if (at == 0)
        return 0;
      else {
      }
      args = at;
      while (input_take(T_COMMA) == 1) {
        at = ast_type();
        if (at == 0)
          return 0;
        else {
        }
        args = ast_link(args, at);
      }
    } else {
    }
    if (input_take(T_RPAREN) == 0)
      return 0;
    else {
    }
    if (input_take(T_COLON) == 0)
      return 0;
    else {
    }
    int ret = ast_type();
    if (ret == 0)
      return 0;
    else {
    }
    return ast_node(TY_CLOSURE, args, ret, 0, 0, 0);
  } else {
  }
  if (input_take(T_FN) == 1) {
    if (input_take(T_LPAREN) == 0)
      return 0;
    else {
    }
    int args = 0;
    if (input_peek() != T_RPAREN) {
      int at = ast_type();
      if (at == 0)
        return 0;
      else {
      }
      args = at;
      while (input_take(T_COMMA) == 1) {
        at = ast_type();
        if (at == 0)
          return 0;
        else {
        }
        args = ast_link(args, at);
      }
    } else {
    }
    if (input_take(T_RPAREN) == 0)
      return 0;
    else {
    }
    if (input_take(T_COLON) == 0)
      return 0;
    else {
    }
    int ret = ast_type();
    if (ret == 0)
      return 0;
    else {
    }
    return ast_node(TY_FUN, args, ret, 0, 0, 0);
  } else {
  }
  if (((input_peek() == T_ARRAY) && ((input_pos + 1) < input_count)) &&
      (input_kind[(input_pos + 1)] == T_SCOPE)) {
    int ns = input_payload();
    input_pos = (input_pos + 2);
    if ((input_peek() != T_ID) && (input_peek() != T_ARRAY))
      return 0;
    else {
    }
    int rhs = input_payload();
    input_pos = (input_pos + 1);
    int named_type = sym_qualified(ns, rhs);
    if (input_take(T_LT) == 1) {
      int args = 0;
      if (input_peek() != T_GT) {
        int at = ast_type();
        if (at == 0)
          return 0;
        else {
        }
        args = at;
        while (input_take(T_COMMA) == 1) {
          at = ast_type();
          if (at == 0)
            return 0;
          else {
          }
          args = ast_link(args, at);
        }
      } else {
      }
      if (input_take(T_GT) == 0)
        return 0;
      else {
      }
      ty = ast_node(TY_GENERIC, args, 0, 0, named_type, 0);
    } else
      ty = ast_node(TY_NAMED, 0, 0, 0, named_type, 0);
  } else {
    if (input_take(T_TINT) == 1)
      base = TY_INT;
    else if (input_take(T_TBOOL) == 1)
      base = TY_BOOL;
    else if (input_take(T_TSTRING) == 1)
      base = TY_STRING;
    else if (input_take(T_TCHAR) == 1)
      base = TY_CHAR;
    else if (input_take(T_FLOAT) == 1)
      base = TY_FLOAT;
    else if (input_take(T_TDOUBLE) == 1)
      base = TY_DOUBLE;
    else if (input_take(T_TLONG) == 1) {
      if (input_take(T_TLONG) == 1)
        base = TY_LLONG;
      else
        base = TY_LONG;
    } else if (input_take(T_TU8) == 1)
      base = TY_U8;
    else if (input_take(T_TU16) == 1)
      base = TY_U16;
    else if (input_take(T_TU32) == 1)
      base = TY_U32;
    else if (input_take(T_TU64) == 1)
      base = TY_U64;
    else if (input_take(T_TI8) == 1)
      base = TY_I8;
    else if (input_take(T_TI16) == 1)
      base = TY_I16;
    else if (input_take(T_TI32) == 1)
      base = TY_I32;
    else if (input_take(T_TI64) == 1)
      base = TY_I64;
    else if (input_take(T_TUSIZE) == 1)
      base = TY_USIZE;
    else if (input_take(T_TVOID) == 1)
      base = TY_VOID;
    else if (input_peek() == T_ID) {
      named = input_payload();
      input_pos = (input_pos + 1);
      if (input_take(T_SCOPE) == 1) {
        if (input_peek() != T_ID)
          return 0;
        else {
        }
        int rhs = input_payload();
        input_pos = (input_pos + 1);
        named = sym_qualified(named, rhs);
      } else
        named = ast_type_name(named);
      if (input_peek() == T_LT) {
        int args = 0;
        input_pos = (input_pos + 1);
        if (input_peek() != T_GT) {
          int at = ast_type();
          if (at == 0)
            return 0;
          else {
          }
          args = at;
          while (input_take(T_COMMA) == 1) {
            at = ast_type();
            if (at == 0)
              return 0;
            else {
            }
            args = ast_link(args, at);
          }
        } else {
        }
        if (input_take(T_GT) == 0)
          return 0;
        else {
        }
        ty = ast_node(TY_GENERIC, args, 0, 0, named, 0);
      } else if (ast_generic_param(named) == 1)
        ty = ast_node(TY_PARAM, 0, 0, 0, named, 0);
      else
        ty = ast_node(TY_NAMED, 0, 0, 0, named, 0);
    } else
      return 0;
    if (ty == 0)
      ty = ast_node(base, 0, 0, 0, named, 0);
    else {
    }
  }
  while (input_take(T_STAR) == 1) {
    ty = ast_node(TY_PTR, ty, 0, 0, 0, 0);
  }
  while (input_take(T_LBRACK) == 1) {
    int size = ast_array_dimension_value();
    if (size < 0)
      return 0;
    else {
    }
    if (input_take(T_RBRACK) == 0)
      return 0;
    else {
    }
    ty = ast_append_array_dimension(ty, size);
  }
  return ty;
}
int ast_call_args(void) {
  int args = 0;
  if (input_peek() != T_RPAREN) {
    int arg = ast_expr();
    if (arg < 0)
      return (0 - 1);
    else {
    }
    args = arg;
    while (input_take(T_COMMA) == 1) {
      arg = ast_expr();
      if (arg < 0)
        return (0 - 1);
      else {
      }
      args = ast_link(args, arg);
    }
  } else {
  }
  if (input_take(T_RPAREN) == 0)
    return (0 - 1);
  else {
  }
  return args;
}
int ast_primary(void) {
  if (input_take(T_FN) == 1) {
    int captures = 0;
    if (input_take(T_LBRACK) == 1) {
      if (input_peek() != T_RBRACK) {
        while (1 == 1) {
          int mode = 2;
          if (input_take(T_MOVE) == 1)
            mode = 1;
          else if (input_take(T_BORROW) == 1)
            mode = 2;
          else if (input_take(T_BORROW_MUT) == 1)
            mode = 3;
          else {
          }
          if (input_peek() != T_ID)
            return (0 - 1);
          else {
          }
          int capture_name = input_payload();
          input_pos = (input_pos + 1);
          int capture = ast_node(N_PARAM, capture_name, 0, 0, 0, mode);
          if (captures == 0)
            captures = capture;
          else
            captures = ast_link(captures, capture);
          if (input_take(T_COMMA) == 0) {
            break;
          } else {
          }
        }
      } else {
      }
      if (input_take(T_RBRACK) == 0)
        return (0 - 1);
      else {
      }
    } else {
    }
    if (input_take(T_LPAREN) == 0)
      return (0 - 1);
    else {
    }
    int params = ast_params();
    if (params < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_RPAREN) == 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_COLON) == 0)
      return (0 - 1);
    else {
    }
    int ret_ty = ast_type();
    if (ret_ty == 0)
      return (0 - 1);
    else {
    }
    int body = ast_stmt();
    if (body < 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_CLOSURE, captures, body, params, ret_ty, 0);
  } else {
  }
  if (input_peek() == T_CHAR) {
    int value = input_payload();
    input_pos = (input_pos + 1);
    return ast_node(N_CHAR, 0, 0, 0, value, 0);
  } else {
  }
  if (input_take(T_NULL) == 1)
    return ast_node(N_NULL, 0, 0, 0, 0, 0);
  else {
  }
  if (input_peek() == T_FLOAT) {
    int value = input_payload();
    input_pos = (input_pos + 1);
    return ast_node(N_FLOAT, 0, 0, 0, value, 0);
  } else {
  }
  if (input_peek() == T_INT) {
    int value = input_payload();
    int raw = input_text_payload();
    input_pos = (input_pos + 1);
    return ast_node(N_INT, 0, 0, 0, value, raw);
  } else {
  }
  if (input_peek() == T_STRING) {
    int value = input_payload();
    input_pos = (input_pos + 1);
    return ast_node(N_STRING, 0, 0, 0, value, 0);
  } else {
  }
  if (input_take(T_TRUE) == 1)
    return ast_node(N_BOOL, 0, 0, 0, 1, 0);
  else {
  }
  if (input_take(T_FALSE) == 1)
    return ast_node(N_BOOL, 0, 0, 0, 0, 0);
  else {
  }
  if ((input_peek() == T_ID) || (input_peek() == T_ARRAY)) {
    int name = input_payload();
    input_pos = (input_pos + 1);
    while (input_take(T_SCOPE) == 1) {
      if ((input_peek() != T_ID) && (input_peek() != T_ARRAY))
        return (0 - 1);
      else {
      }
      int rhs = input_payload();
      input_pos = (input_pos + 1);
      name = sym_qualified(name, rhs);
    }
    int explicit_args = 0;
    int explicit_valid = 0;
    int generic_probe = input_pos;
    if (input_take(T_LT) == 1) {
      if (input_peek() != T_GT) {
        int type_arg = ast_type();
        if (type_arg != 0) {
          explicit_args = type_arg;
          while (input_take(T_COMMA) == 1) {
            type_arg = ast_type();
            if (type_arg == 0) {
              explicit_args = 0;
              { break; }
            } else {
            }
            explicit_args = ast_link(explicit_args, type_arg);
          }
          if ((explicit_args != 0) && (input_take(T_GT) == 1))
            explicit_valid = 1;
          else {
          }
        } else {
        }
      } else {
      }
      if (explicit_valid == 0) {
        input_pos = generic_probe;
        explicit_args = 0;
      } else {
      }
    } else {
    }
    if (explicit_valid == 1) {
      if (input_take(T_LPAREN) == 0) {
        input_pos = generic_probe;
        explicit_valid = 0;
        explicit_args = 0;
      } else {
      }
    } else {
    }
    if ((explicit_valid == 0) && (input_take(T_LPAREN) == 1)) {
    } else {
    }
    if ((explicit_valid == 1) || (input_kind[(input_pos - 1)] == T_LPAREN)) {
      int args = ast_call_args();
      if (args < 0)
        return (0 - 1);
      else {
      }
      return ast_node(N_CALL, args, explicit_args, 0, name, 0);
    } else {
    }
    int base = ast_node(N_VAR, 0, 0, 0, name, 0);
    if (input_take(T_DOT) == 1) {
      if (input_peek() != T_ID)
        return (0 - 1);
      else {
      }
      int field = input_payload();
      input_pos = (input_pos + 1);
      int field_expr = ast_node(N_FIELD_ACCESS, base, 0, 0, field, 0);
      if (input_take(T_LPAREN) == 1) {
        int args = ast_call_args();
        if (args < 0)
          return (0 - 1);
        else {
        }
        return ast_node(N_INDIRECT_CALL, field_expr, args, 0, 0, 0);
      } else {
      }
      return field_expr;
    } else {
    }
    return base;
  } else {
  }
  if (input_take(T_LPAREN) == 1) {
    int e = ast_expr();
    if (e < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_COMMA) == 1) {
      int items = e;
      while (1 == 1) {
        int item = ast_expr();
        if (item < 0)
          return (0 - 1);
        else {
        }
        items = ast_link(items, item);
        if (input_take(T_COMMA) == 0) {
          {
            break;
          }
        } else {
        }
      }
      if (input_take(T_RPAREN) == 0)
        return (0 - 1);
      else {
      }
      return ast_node(N_TUPLE, items, 0, 0, 0, 0);
    } else {
    }
    if (input_take(T_RPAREN) == 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_LPAREN) == 1) {
      int args = ast_call_args();
      if (args < 0)
        return (0 - 1);
      else {
      }
      return ast_node(N_INDIRECT_CALL, e, args, 0, 0, 0);
    } else {
    }
    return e;
  } else {
  }
  return (0 - 1);
}
int ast_unary(void) {
  if (input_take(T_MINUS) == 1) {
    int e = ast_unary();
    if (e < 0)
      return (0 - 1);
    else {
    }
    int zero = ast_node(N_INT, 0, 0, 0, 0, 0);
    return ast_node(N_BINOP, zero, e, 0, OP_SUB, 0);
  } else {
  }
  if (input_take(T_BITNOT) == 1) {
    int e = ast_unary();
    if (e < 0)
      return (0 - 1);
    else {
    }
    int allbits = ast_node(N_INT, 0, 0, 0, (0 - 1), 0);
    return ast_node(N_BINOP, e, allbits, 0, OP_BITXOR, 0);
  } else {
  }
  if (input_take(T_NOT) == 1) {
    int e = ast_unary();
    if (e < 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_UNARY, e, 0, 0, OP_NOT, 0);
  } else {
  }
  if (input_take(T_MOVE) == 1) {
    int e = ast_unary();
    if (e < 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_MOVE, e, 0, 0, 0, 0);
  } else {
  }
  if (input_take(T_STAR) == 1) {
    int e = ast_unary();
    if (e < 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_DEREF, e, 0, 0, 0, 0);
  } else {
  }
  if (input_take(T_AMP) == 1) {
    int mutable = 0;
    if (input_take(T_MUT) == 1)
      mutable = 1;
    else {
    }
    int e = ast_unary();
    if (e < 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_ADDRESS, e, 0, 0, mutable, 0);
  } else {
  }
  return ast_primary();
}
int ast_precedence(int kind) {
  if (kind == T_OR_OR)
    return 1;
  else {
  }
  if (kind == T_AND_AND)
    return 2;
  else {
  }
  if (kind == T_EQEQ)
    return 3;
  else {
  }
  if (kind == T_NEQ)
    return 3;
  else {
  }
  if (kind == T_LT)
    return 3;
  else {
  }
  if (kind == T_GT)
    return 3;
  else {
  }
  if (kind == T_LE)
    return 3;
  else {
  }
  if (kind == T_GE)
    return 3;
  else {
  }
  if (kind == T_CONCAT)
    return 4;
  else {
  }
  if (kind == T_BITOR)
    return 5;
  else {
  }
  if (kind == T_BITXOR)
    return 6;
  else {
  }
  if (kind == T_AMP)
    return 7;
  else {
  }
  if (kind == T_SHL)
    return 8;
  else {
  }
  if (kind == T_SHR)
    return 8;
  else {
  }
  if (kind == T_PLUS)
    return 9;
  else {
  }
  if (kind == T_MINUS)
    return 9;
  else {
  }
  if (kind == T_STAR)
    return 10;
  else {
  }
  if (kind == T_DIVIDE)
    return 10;
  else {
  }
  if (kind == T_MOD)
    return 10;
  else {
  }
  return 0;
}
int ast_operator(int kind) {
  if (kind == T_PLUS)
    return OP_ADD;
  else {
  }
  if (kind == T_MINUS)
    return OP_SUB;
  else {
  }
  if (kind == T_STAR)
    return OP_MUL;
  else {
  }
  if (kind == T_DIVIDE)
    return OP_DIV;
  else {
  }
  if (kind == T_MOD)
    return OP_MOD;
  else {
  }
  if (kind == T_EQEQ)
    return OP_EQ;
  else {
  }
  if (kind == T_NEQ)
    return OP_NEQ;
  else {
  }
  if (kind == T_LT)
    return OP_LT;
  else {
  }
  if (kind == T_GT)
    return OP_GT;
  else {
  }
  if (kind == T_LE)
    return OP_LE;
  else {
  }
  if (kind == T_GE)
    return OP_GE;
  else {
  }
  if (kind == T_AND_AND)
    return OP_AND;
  else {
  }
  if (kind == T_OR_OR)
    return OP_OR;
  else {
  }
  if (kind == T_CONCAT)
    return OP_CONCAT;
  else {
  }
  if (kind == T_AMP)
    return OP_BITAND;
  else {
  }
  if (kind == T_BITOR)
    return OP_BITOR;
  else {
  }
  if (kind == T_BITXOR)
    return OP_BITXOR;
  else {
  }
  if (kind == T_SHL)
    return OP_SHL;
  else {
  }
  if (kind == T_SHR)
    return OP_SHR;
  else {
  }
  return OP_GT;
}
int ast_compound_operator(int kind) {
  if (kind == T_PLUS_EQ)
    return OP_ADD;
  else {
  }
  if (kind == T_MINUS_EQ)
    return OP_SUB;
  else {
  }
  if (kind == T_STAR_EQ)
    return OP_MUL;
  else {
  }
  if (kind == T_DIV_EQ)
    return OP_DIV;
  else {
  }
  if (kind == T_MOD_EQ)
    return OP_MOD;
  else {
  }
  if (kind == T_AMP_EQ)
    return OP_BITAND;
  else {
  }
  if (kind == T_BITOR_EQ)
    return OP_BITOR;
  else {
  }
  if (kind == T_BITXOR_EQ)
    return OP_BITXOR;
  else {
  }
  if (kind == T_SHL_EQ)
    return OP_SHL;
  else {
  }
  if (kind == T_SHR_EQ)
    return OP_SHR;
  else {
  }
  return 0;
}
int ast_take_compound_operator(void) {
  int kind = input_peek();
  int op = ast_compound_operator(kind);
  if (op != 0)
    input_pos = (input_pos + 1);
  else {
  }
  return op;
}
int ast_compound_assign(int left, int op, int right) {
  return ast_node(N_COMPOUND_ASSIGN, left, right, 0, op, 0);
}
int ast_expr_prec(int min_prec) {
  int left = ast_unary();
  if (left < 0)
    return (0 - 1);
  else {
  }
  while (1 == 1) {
    if (input_take(T_LBRACK) == 1) {
      int index = ast_expr();
      if (index < 0)
        return (0 - 1);
      else {
      }
      if (input_take(T_RBRACK) == 0)
        return (0 - 1);
      else {
      }
      left = ast_node(N_INDEX, left, index, 0, 0, 0);
    } else if (input_take(T_DOT) == 1) {
      if (input_peek() != T_ID)
        return (0 - 1);
      else {
      }
      int field = input_payload();
      input_pos = (input_pos + 1);
      left = ast_node(N_FIELD_ACCESS, left, 0, 0, field, 0);
    } else {
      int p = ast_precedence(input_peek());
      if (p < min_prec)
        return left;
      else {
      }
      int op_token = input_peek();
      input_pos = (input_pos + 1);
      int right = ast_expr_prec((p + 1));
      if (right < 0)
        return (0 - 1);
      else {
      }
      left = ast_node(N_BINOP, left, right, 0, ast_operator(op_token), 0);
    }
  }
  return left;
}
int ast_expr(void) {
  return ast_expr_prec(1);
}
int clone_for_step(int step) {
  return ast_node(node_kind[step], node_a[step], node_b[step], node_c[step], node_value[step],
                  node_aux[step]);
}
int lower_for_stmt(int id, int step) {
  if (node_kind[id] == N_CONTINUE) {
    int s = clone_for_step(step);
    int c = ast_node(N_CONTINUE, 0, 0, 0, 0, 0);
    return ast_node(N_BLOCK, ast_link(s, c), 0, 0, 0, 0);
  } else {
  }
  if (node_kind[id] == N_IF) {
    int yes = lower_for_stmt(node_b[id], step);
    int no = lower_for_stmt(node_c[id], step);
    return ast_node(N_IF, node_a[id], yes, no, node_value[id], node_aux[id]);
  } else {
  }
  if (node_kind[id] == N_BLOCK) {
    int item = node_a[id];
    int out = 0;
    while (item != 0) {
      int x = lower_for_stmt(item, step);
      if (out == 0)
        out = x;
      else
        out = ast_link(out, x);
      item = node_next[item];
    }
    return ast_node(N_BLOCK, out, 0, 0, 0, 0);
  } else {
  }
  return id;
}
int ast_alignment(void) {
  if (input_take(T_ALIGNAS) == 0)
    return 0;
  else {
  }
  if (input_take(T_LPAREN) == 0)
    return (0 - 1);
  else {
  }
  if (input_peek() != T_INT)
    return (0 - 1);
  else {
  }
  int alignment = input_payload();
  input_pos = (input_pos + 1);
  if (alignment < 1)
    return (0 - 1);
  else {
  }
  int power = 1;
  while (power < alignment) {
    if (power > 1073741824)
      return (0 - 1);
    else {
    }
    power = (power * 2);
  }
  if (power != alignment)
    return (0 - 1);
  else {
  }
  if (input_take(T_RPAREN) == 0)
    return (0 - 1);
  else {
  }
  return alignment;
}
int ast_stmt(void) {
  if (input_take(T_DEFER) == 1) {
    int cleanup = ast_expr();
    if (cleanup < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_DEFER, cleanup, 0, 0, 0, 0);
  } else {
  }
  if (input_take(T_MATCH) == 1) {
    int subject = ast_expr();
    if (subject < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_LBRACE) == 0)
      return (0 - 1);
    else {
    }
    int arms = 0;
    while (input_peek() != T_RBRACE) {
      if ((input_peek() == T_EOF) || (input_peek() != T_ID))
        return (0 - 1);
      else {
      }
      int variant = input_payload();
      input_pos = (input_pos + 1);
      int is_default = ast_is_match_default(variant);
      if (is_default == 1)
        variant = 0;
      else {
      }
      int bindings = 0;
      if (input_take(T_LPAREN) == 1) {
        if (is_default == 1)
          return (0 - 1);
        else {
        }
        if (input_peek() != T_ID)
          return (0 - 1);
        else {
        }
        int binding_name = input_payload();
        input_pos = (input_pos + 1);
        bindings = ast_node(N_VAR, 0, 0, 0, binding_name, 0);
        if (input_take(T_COMMA) == 1) {
          if (input_peek() != T_ID)
            return (0 - 1);
          else {
          }
          int second_binding_name = input_payload();
          input_pos = (input_pos + 1);
          int second_binding = ast_node(N_VAR, 0, 0, 0, second_binding_name, 0);
          bindings = ast_link(bindings, second_binding);
        } else {
        }
        if (input_take(T_RPAREN) == 0)
          return (0 - 1);
        else {
        }
      } else {
      }
      if (input_take(T_FATARROW) == 0)
        return (0 - 1);
      else {
      }
      int body = ast_stmt();
      if (body < 0)
        return (0 - 1);
      else {
      }
      int arm = ast_node(N_MATCH_ARM, bindings, body, 0, variant, 0);
      if (arms == 0)
        arms = arm;
      else
        arms = ast_link(arms, arm);
    }
    if (input_take(T_RBRACE) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_MATCH, subject, arms, 0, 0, 0);
  } else {
  }
  if (input_take(T_BREAK) == 1) {
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_BREAK, 0, 0, 0, 0, 0);
  } else {
  }
  if (input_take(T_CONTINUE) == 1) {
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_CONTINUE, 0, 0, 0, 0, 0);
  } else {
  }
  if (input_take(T_LET) == 1) {
    if (input_take(T_LPAREN) == 1) {
      int bindings = 0;
      int binding_count = 0;
      if (input_peek() != T_ID)
        return (0 - 1);
      else {
      }
      while (1 == 1) {
        int binding_name = input_payload();
        input_pos = (input_pos + 1);
        int binding = ast_node(N_VAR, 0, 0, 0, binding_name, 0);
        if (bindings == 0)
          bindings = binding;
        else
          bindings = ast_link(bindings, binding);
        binding_count = (binding_count + 1);
        if (input_take(T_COMMA) == 0) {
          break;
        } else {
        }
        if (input_peek() != T_ID)
          return (0 - 1);
        else {
        }
      }
      if (binding_count < 2)
        return (0 - 1);
      else {
      }
      if (input_take(T_RPAREN) == 0)
        return (0 - 1);
      else {
      }
      if (input_take(T_COLON) == 0)
        return (0 - 1);
      else {
      }
      int tuple_ty = ast_type();
      if (tuple_ty == 0)
        return (0 - 1);
      else {
      }
      if (input_take(T_EQUAL) == 0)
        return (0 - 1);
      else {
      }
      int tuple_value = ast_expr();
      if (tuple_value < 0)
        return (0 - 1);
      else {
      }
      if (input_take(T_SEMI) == 0)
        return (0 - 1);
      else {
      }
      return ast_node(N_TUPLE_BIND, bindings, tuple_ty, tuple_value, 0, 0);
    } else {
    }
    if (input_peek() != T_ID)
      return (0 - 1);
    else {
    }
    int name = input_payload();
    input_pos = (input_pos + 1);
    if (input_take(T_COLON) == 0)
      return (0 - 1);
    else {
    }
    int ty = ast_type();
    if (ty == 0)
      return (0 - 1);
    else {
    }
    int alignment = ast_alignment();
    if (alignment < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_EQUAL) == 0)
      return (0 - 1);
    else {
    }
    int value = ast_expr();
    if (value < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_LET, name, ty, value, 0, alignment);
  } else {
  }
  if (input_take(T_PRINT) == 1) {
    int value = ast_expr();
    if (value < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_PRINT, value, 0, 0, 0, 0);
  } else {
  }
  if (input_take(T_PRINTLN) == 1) {
    int value = ast_expr();
    if (value < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_PRINTLN, value, 0, 0, 0, 0);
  } else {
  }
  if (input_take(T_IF) == 1) {
    int cond = ast_expr();
    if (cond < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_THEN) == 0)
      return (0 - 1);
    else {
    }
    int yes = ast_stmt();
    if (yes < 0)
      return (0 - 1);
    else {
    }
    int no = 0;
    if (input_take(T_ELSE) == 1) {
      no = ast_stmt();
      if (no < 0)
        return (0 - 1);
      else {
      }
    } else {
      no = ast_node(N_BLOCK, 0, 0, 0, 0, 0);
    }
    return ast_node(N_IF, cond, yes, no, 0, 0);
  } else {
  }
  if (input_take(T_FOR) == 1) {
    if (input_take(T_LPAREN) == 0)
      return (0 - 1);
    else {
    }
    int init = 0;
    if (input_peek() != T_SEMI) {
      if (input_take(T_LET) == 1) {
        if (input_peek() != T_ID)
          return (0 - 1);
        else {
        }
        int n = input_payload();
        input_pos = (input_pos + 1);
        if (input_take(T_COLON) == 0)
          return (0 - 1);
        else {
        }
        int t = ast_type();
        if (t == 0)
          return (0 - 1);
        else {
        }
        int alignment = ast_alignment();
        if (alignment < 0)
          return (0 - 1);
        else {
        }
        if (input_take(T_EQUAL) == 0)
          return (0 - 1);
        else {
        }
        int v = ast_expr();
        init = ast_node(N_LET, n, t, v, 0, alignment);
      } else {
        int l = ast_expr();
        if (l < 0)
          return (0 - 1);
        else {
        }
        if (input_take(T_EQUAL) == 1) {
          int r = ast_expr();
          init = ast_node(N_ASSIGN, l, r, 0, 0, 0);
        } else {
          int cop = ast_take_compound_operator();
          if (cop == 0)
            return (0 - 1);
          else {
          }
          int r = ast_expr();
          init = ast_compound_assign(l, cop, r);
        }
      }
    } else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    int cond = ast_expr();
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    int step = 0;
    if (input_peek() != T_RPAREN) {
      int l = ast_expr();
      if (l < 0)
        return (0 - 1);
      else {
      }
      if (input_take(T_EQUAL) == 1) {
        int r = ast_expr();
        step = ast_node(N_ASSIGN, l, r, 0, 0, 0);
      } else {
        int cop = ast_take_compound_operator();
        if (cop != 0) {
          int r = ast_expr();
          step = ast_compound_assign(l, cop, r);
        } else
          step = ast_node(N_EXPR, l, 0, 0, 0, 0);
      }
    } else
      step = ast_node(N_BLOCK, 0, 0, 0, 0, 0);
    if (input_take(T_RPAREN) == 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_LBRACE) == 0)
      return (0 - 1);
    else {
    }
    for_step_context = step;
    int items = 0;
    while (input_peek() != T_RBRACE) {
      if (input_peek() == T_EOF)
        return (0 - 1);
      else {
      }
      int x = ast_stmt();
      if (x < 0)
        return (0 - 1);
      else {
      }
      if (items == 0)
        items = x;
      else
        items = ast_link(items, x);
    }
    if (input_take(T_RBRACE) == 0)
      return (0 - 1);
    else {
    }
    for_step_context = 0;
    int body = ast_node(N_BLOCK, items, 0, 0, 0, 0);
    return ast_node(N_FOR, init, cond, body, step, 0);
  } else {
  }
  if (input_take(T_WHILE) == 1) {
    int cond = ast_expr();
    if (cond < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_LBRACE) == 0)
      return (0 - 1);
    else {
    }
    int items = 0;
    while (input_peek() != T_RBRACE) {
      if (input_peek() == T_EOF)
        return (0 - 1);
      else {
      }
      int item = ast_stmt();
      if (item < 0)
        return (0 - 1);
      else {
      }
      if (items == 0)
        items = item;
      else
        items = ast_link(items, item);
    }
    if (input_take(T_RBRACE) == 0)
      return (0 - 1);
    else {
    }
    int body = ast_node(N_BLOCK, items, 0, 0, 0, 0);
    return ast_node(N_WHILE, cond, body, 0, 0, 0);
  } else {
  }
  if (input_take(T_LBRACE) == 1) {
    int items = 0;
    while (input_peek() != T_RBRACE) {
      if (input_peek() == T_EOF)
        return (0 - 1);
      else {
      }
      int item = ast_stmt();
      if (item < 0)
        return (0 - 1);
      else {
      }
      if (items == 0)
        items = item;
      else
        items = ast_link(items, item);
    }
    if (input_take(T_RBRACE) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_BLOCK, items, 0, 0, 0, 0);
  } else {
  }
  if (input_take(T_RETURN) == 1) {
    int value = 0;
    if (input_peek() != T_SEMI) {
      value = ast_expr();
      if (value < 0)
        return (0 - 1);
      else {
      }
    } else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    int return_stmt = ast_node(N_RETURN, value, 0, 0, 0, 0);
    if (value != 0)
      node_pos[return_stmt] = node_pos[value];
    else {
    }
    return return_stmt;
  } else {
  }
  int left = ast_expr();
  if (left < 0)
    return (0 - 1);
  else {
  }
  if (input_take(T_EQUAL) == 1) {
    int right = ast_expr();
    if (right < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_ASSIGN, left, right, 0, 0, 0);
  } else {
  }
  int compound_op = ast_take_compound_operator();
  if (compound_op != 0) {
    int right_compound = ast_expr();
    if (right_compound < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_compound_assign(left, compound_op, right_compound);
  } else {
  }
  if (input_take(T_SEMI) == 0)
    return (0 - 1);
  else {
  }
  return ast_node(N_EXPR, left, 0, 0, 0, 0);
}
int ast_params(void) {
  int params = 0;
  if (input_peek() == T_RPAREN)
    return 0;
  else {
  }
  while (1 == 1) {
    int mode = 0;
    if (input_take(T_MOVE) == 1)
      mode = 1;
    else if (input_take(T_BORROW) == 1)
      mode = 2;
    else if (input_take(T_BORROW_MUT) == 1)
      mode = 3;
    else {
    }
    if (input_peek() != T_ID)
      return (0 - 1);
    else {
    }
    int name = input_payload();
    input_pos = (input_pos + 1);
    if (input_take(T_COLON) == 0)
      return (0 - 1);
    else {
    }
    int ty = ast_type();
    if (ty == 0)
      return (0 - 1);
    else {
    }
    int param = ast_node(N_PARAM, name, ty, 0, 0, mode);
    if (params == 0)
      params = param;
    else
      params = ast_link(params, param);
    if (input_take(T_COMMA) == 0)
      return params;
    else {
    }
  }
}
int ast_struct_decl(void) {
  if (input_peek() != T_ID)
    return (0 - 1);
  else {
  }
  int name = input_payload();
  input_pos = (input_pos + 1);
  name = ast_decl_name(name);
  int params = 0;
  int old_scope = ast_generic_scope;
  if (input_peek() == T_LT) {
    params = ast_generic_params();
    if (params < 0)
      return (0 - 1);
    else {
    }
    ast_generic_scope = params;
  } else {
  }
  if (input_take(T_LBRACE) == 0)
    return (0 - 1);
  else {
  }
  int fields = 0;
  while (input_peek() != T_RBRACE) {
    if (input_peek() == T_EOF)
      return (0 - 1);
    else {
    }
    if (input_peek() != T_ID)
      return (0 - 1);
    else {
    }
    int field_name = input_payload();
    input_pos = (input_pos + 1);
    if (input_take(T_COLON) == 0)
      return (0 - 1);
    else {
    }
    int field_type = ast_type();
    if (field_type == 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    int field = ast_node(N_FIELD, field_name, field_type, 0, 0, 0);
    if (fields == 0)
      fields = field;
    else
      fields = ast_link(fields, field);
  }
  if (input_take(T_RBRACE) == 0)
    return (0 - 1);
  else {
  }
  if (input_take(T_SEMI) == 1) {
  } else {
  }
  ast_generic_scope = old_scope;
  if (params == 0)
    return ast_node(N_STRUCT, fields, 0, 0, name, 0);
  else {
  }
  return ast_node(N_GENERIC_STRUCT, fields, 0, params, name, 0);
}
int ast_enum_decl(void) {
  if (input_peek() != T_ID)
    return (0 - 1);
  else {
  }
  int name = input_payload();
  input_pos = (input_pos + 1);
  name = ast_decl_name(name);
  if (input_take(T_LBRACE) == 0)
    return (0 - 1);
  else {
  }
  int values = 0;
  int ordinal = 0;
  while (input_peek() != T_RBRACE) {
    if (input_peek() == T_EOF)
      return (0 - 1);
    else {
    }
    if (input_peek() != T_ID)
      return (0 - 1);
    else {
    }
    int member = input_payload();
    input_pos = (input_pos + 1);
    int payload = 0;
    if (input_take(T_LBRACE) == 1) {
      while (input_peek() != T_RBRACE) {
        if ((input_peek() == T_EOF) || (input_peek() != T_ID))
          return (0 - 1);
        else {
        }
        int field_name = input_payload();
        input_pos = (input_pos + 1);
        if (input_take(T_COLON) == 0)
          return (0 - 1);
        else {
        }
        int field_type = ast_type();
        if ((field_type == 0) || (input_take(T_SEMI) == 0))
          return (0 - 1);
        else {
        }
        int field = ast_node(N_FIELD, field_name, field_type, 0, 0, 0);
        if (payload == 0)
          payload = field;
        else
          payload = ast_link(payload, field);
      }
      if (input_take(T_RBRACE) == 0)
        return (0 - 1);
      else {
      }
    } else {
    }
    int item = ast_node(N_FIELD, member, payload, 0, ordinal, 0);
    if (values == 0)
      values = item;
    else
      values = ast_link(values, item);
    ordinal = (ordinal + 1);
    if (input_take(T_COMMA) == 0) {
    } else {
    }
  }
  if (input_take(T_RBRACE) == 0)
    return (0 - 1);
  else {
  }
  if (input_take(T_SEMI) == 1) {
  } else {
  }
  return ast_node(N_ENUM, values, 0, 0, name, 0);
}
int ast_namespace_decl(void) {
  if ((input_peek() != T_ID) && (input_peek() != T_ARRAY))
    return (0 - 1);
  else {
  }
  int raw = input_payload();
  input_pos = (input_pos + 1);
  int ns = raw;
  if (ast_namespace_scope != 0)
    ns = sym_qualified(ast_namespace_scope, raw);
  else {
  }
  if (input_take(T_LBRACE) == 0)
    return (0 - 1);
  else {
  }
  int old_ns = ast_namespace_scope;
  ast_namespace_scope = ns;
  int items = 0;
  while (input_peek() != T_RBRACE) {
    if (input_peek() == T_EOF) {
      ast_namespace_scope = old_ns;
      return (0 - 1);
    } else {
    }
    int item = ast_decl();
    if (item < 0) {
      ast_namespace_scope = old_ns;
      return (0 - 1);
    } else {
    }
    if (items == 0)
      items = item;
    else
      items = ast_link(items, item);
  }
  if (input_take(T_RBRACE) == 0) {
    ast_namespace_scope = old_ns;
    return (0 - 1);
  } else {
  }
  if (input_take(T_SEMI) == 1) {
  } else {
  }
  ast_namespace_scope = old_ns;
  return ast_node(N_LIST, items, 0, 0, 0, 0);
}
int ast_decl(void) {
  if (input_take(T_NAMESPACE) == 1)
    return ast_namespace_decl();
  else {
  }
  if (input_take(T_EXTERN) == 1) {
    int header_id = 0;
    if (input_peek() == T_STRING) {
      header_id = input_payload();
      input_pos = (input_pos + 1);
    } else {
    }
    if (input_take(T_FUNC) == 0)
      return (0 - 1);
    else {
    }
    if (input_peek() != T_ID)
      return (0 - 1);
    else {
    }
    int name = input_payload();
    input_pos = (input_pos + 1);
    name = ast_decl_name(name);
    if (input_take(T_LPAREN) == 0)
      return (0 - 1);
    else {
    }
    int params = ast_params();
    if (params < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_RPAREN) == 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_COLON) == 0)
      return (0 - 1);
    else {
    }
    int ret_ty = ast_type();
    if (ret_ty == 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_EXTERN, header_id, ret_ty, params, name, node_kind[ret_ty]);
  } else {
  }
  if (input_take(T_CONST) == 1) {
    if (input_peek() != T_ID)
      return (0 - 1);
    else {
    }
    int name = input_payload();
    input_pos = (input_pos + 1);
    name = ast_decl_name(name);
    if (input_take(T_COLON) == 0)
      return (0 - 1);
    else {
    }
    int ty = ast_type();
    if (ty == 0)
      return (0 - 1);
    else {
    }
    int alignment = ast_alignment();
    if (alignment < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_EQUAL) == 0)
      return (0 - 1);
    else {
    }
    int value = ast_expr();
    if (value < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    int const_value = ast_const_eval(value);
    if ((node_kind[ty] == TY_INT) && (const_value >= 0))
      (void)(ast_const_add(name, const_value));
    else {
    }
    return ast_node(N_CONST, name, ty, value, 0, alignment);
  } else {
  }
  if (input_take(T_STRUCT) == 1)
    return ast_struct_decl();
  else {
  }
  if (input_take(T_ENUM) == 1)
    return ast_enum_decl();
  else {
  }
  if (input_take(T_LET) == 1) {
    if (input_peek() != T_ID)
      return (0 - 1);
    else {
    }
    int name = input_payload();
    input_pos = (input_pos + 1);
    name = ast_decl_name(name);
    if (input_take(T_COLON) == 0)
      return (0 - 1);
    else {
    }
    int ty = ast_type();
    if (ty == 0)
      return (0 - 1);
    else {
    }
    int alignment = ast_alignment();
    if (alignment < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_EQUAL) == 0)
      return (0 - 1);
    else {
    }
    int value = ast_expr();
    if (value < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_SEMI) == 0)
      return (0 - 1);
    else {
    }
    return ast_node(N_GLOBAL, name, ty, value, 0, alignment);
  } else {
  }
  if (input_take(T_FUNC) == 1) {
    if (input_peek() != T_ID)
      return (0 - 1);
    else {
    }
    int name = input_payload();
    input_pos = (input_pos + 1);
    name = ast_decl_name(name);
    int generic_params = 0;
    int old_scope = ast_generic_scope;
    if (input_peek() == T_LT) {
      generic_params = ast_generic_params();
      if (generic_params < 0)
        return (0 - 1);
      else {
      }
      ast_generic_scope = generic_params;
    } else {
    }
    if (input_take(T_LPAREN) == 0)
      return (0 - 1);
    else {
    }
    int params = ast_params();
    if (params < 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_RPAREN) == 0)
      return (0 - 1);
    else {
    }
    if (input_take(T_COLON) == 0)
      return (0 - 1);
    else {
    }
    int ret_ty = ast_type();
    if (ret_ty == 0)
      return (0 - 1);
    else {
    }
    int body = ast_stmt();
    if (body < 0)
      return (0 - 1);
    else {
    }
    ast_generic_scope = old_scope;
    if (generic_params == 0)
      return ast_node(N_FUNC, body, ret_ty, params, name, node_kind[ret_ty]);
    else {
    }
    return ast_node(N_GENERIC_FUNC, body, ret_ty, params, name, generic_params);
  } else {
  }
  return (0 - 1);
}
int ast_flatten_decl_list(int item) {
  int head = 0;
  int tail = 0;
  int p = item;
  while (p != 0) {
    int next = node_next[p];
    node_next[p] = 0;
    int part = p;
    if (node_kind[p] == N_LIST)
      part = ast_flatten_decl_list(node_a[p]);
    else {
    }
    if (part != 0) {
      if (head == 0) {
        head = part;
        tail = part;
      } else {
        node_next[tail] = part;
      }
      while (node_next[tail] != 0) {
        tail = node_next[tail];
      }
    } else {
    }
    p = next;
  }
  return head;
}
int ast_program(void) {
  int items = 0;
  while (input_peek() != T_EOF) {
    int item = ast_decl();
    if (item < 0)
      return (0 - 1);
    else {
    }
    int flat = 0;
    if (node_kind[item] == N_LIST)
      flat = ast_flatten_decl_list(node_a[item]);
    else
      flat = item;
    if (flat != 0) {
      if (items == 0)
        items = flat;
      else {
        int tail = items;
        while (node_next[tail] != 0) {
          tail = node_next[tail];
        }
        node_next[tail] = flat;
      }
    } else {
    }
  }
  return ast_node(N_PROGRAM, items, 0, 0, 0, 0);
}
void c_source_reset(void) {
  c_source_len = 0;
}
void ensure_c_source(int need) {
  if (need < c_source_cap)
    return;
  else {
  }
  int n = next_capacity(c_source_cap, need);
  c_source = grow_ints(c_source, c_source_cap, n);
  c_source_cap = n;
}
void c_source_put(int c) {
  (void)(ensure_c_source(c_source_len));
  c_source[c_source_len] = c;
  c_source_len = (c_source_len + 1);
}
void ensure_source_file_names(int need) {
  if (need < source_file_cap)
    return;
  else {
  }
  int n = next_capacity(source_file_cap, need);
  source_file_name_start = grow_ints(source_file_name_start, source_file_cap, n);
  source_file_name_len = grow_ints(source_file_name_len, source_file_cap, n);
  source_file_cap = n;
}
void ensure_source_file_text(int need) {
  if (need < source_file_name_text_cap)
    return;
  else {
  }
  int n = next_capacity(source_file_name_text_cap, need);
  source_file_name_text = grow_ints(source_file_name_text, source_file_name_text_cap, n);
  source_file_name_text_cap = n;
}
int source_path_length(char *path) {
  int n = 0;
  while (path[n] != 0) {
    n = (n + 1);
  }
  return n;
}
int source_file_intern(char *path) {
  int length = source_path_length(path);
  int id = 1;
  while (id < source_file_count) {
    if (source_file_name_len[id] == length) {
      int i = 0;
      int same = 1;
      while (i < length) {
        if (source_file_name_text[(source_file_name_start[id] + i)] != path[i])
          same = 0;
        else {
        }
        i = (i + 1);
      }
      if (same == 1)
        return id;
      else {
      }
    } else {
    }
    id = (id + 1);
  }
  id = source_file_count;
  (void)(ensure_source_file_names(id));
  (void)(ensure_source_file_text((source_file_name_text_len + length)));
  source_file_name_start[id] = source_file_name_text_len;
  source_file_name_len[id] = length;
  int j = 0;
  while (j < length) {
    source_file_name_text[(source_file_name_text_len + j)] = path[j];
    j = (j + 1);
  }
  source_file_name_text_len = (source_file_name_text_len + length);
  source_file_count = (source_file_count + 1);
  return id;
}
void ensure_source_deps(int need) {
  if (need < source_dep_cap)
    return;
  else {
  }
  int n = next_capacity(source_dep_cap, need);
  source_dep_from = grow_ints(source_dep_from, source_dep_cap, n);
  source_dep_to = grow_ints(source_dep_to, source_dep_cap, n);
  source_dep_cap = n;
}
void source_dep_add(int from_id, int to_id) {
  if ((from_id < 1) || (to_id < 1))
    return;
  else {
  }
  int i = 0;
  while (i < source_dep_count) {
    if ((source_dep_from[i] == from_id) && (source_dep_to[i] == to_id))
      return;
    else {
    }
    i = (i + 1);
  }
  (void)(ensure_source_deps(source_dep_count));
  source_dep_from[source_dep_count] = from_id;
  source_dep_to[source_dep_count] = to_id;
  source_dep_count = (source_dep_count + 1);
}
int source_dep_reaches(int current, int target) {
  int capacity = (source_file_count + 1);
  int *work = alloc_ints(capacity);
  int *seen = alloc_ints(capacity);
  int work_count = 1;
  int found = 0;
  work[0] = current;
  if ((current >= 0) && (current < capacity))
    seen[current] = 1;
  else {
  }
  while ((work_count > 0) && (found == 0)) {
    work_count = (work_count - 1);
    int item = work[work_count];
    if (item == target)
      found = 1;
    else {
      int i = 0;
      while (i < source_dep_count) {
        if (source_dep_from[i] == item) {
          int next = source_dep_to[i];
          if (((next >= 0) && (next < capacity)) && (seen[next] == 0)) {
            seen[next] = 1;
            work[work_count] = next;
            work_count = (work_count + 1);
          } else {
          }
        } else {
        }
        i = (i + 1);
      }
    }
  }
  (void)(free_ints(work));
  (void)(free_ints(seen));
  return found;
}
void source_import_fail(int kind, int file_id, int target_id, int line) {
  if (include_ok == 0)
    return;
  else {
  }
  include_ok = 0;
  source_import_error_kind = kind;
  source_import_error_file = file_id;
  source_import_error_target = target_id;
  source_import_error_line = line;
}
void source_reset(void) {
  source_len = 0;
  source_pos = 0;
  source_file_count = 1;
  source_file_name_text_len = 0;
  source_active_file = 0;
  source_active_line = 1;
  source_dep_count = 0;
  source_import_error_kind = 0;
  source_import_error_file = 0;
  source_import_error_target = 0;
  source_import_error_line = 1;
}
void source_put(int c) {
  (void)(ensure_source(source_len));
  source[source_len] = c;
  source_file_at[source_len] = source_active_file;
  source_line_at[source_len] = source_active_line;
  source_len = (source_len + 1);
  if (c == 10)
    source_active_line = (source_active_line + 1);
  else {
  }
}
int is_space(int c) {
  if (c == 32)
    return 1;
  else {
  }
  if (c == 9)
    return 1;
  else {
  }
  if (c == 10)
    return 1;
  else {
  }
  if (c == 13)
    return 1;
  else {
  }
  return 0;
}
int is_digit(int c) {
  if (c < 48)
    return 0;
  else {
  }
  if (c > 57)
    return 0;
  else {
  }
  return 1;
}
int hex_digit_value(int c) {
  if ((c >= 48) && (c <= 57))
    return (c - 48);
  else {
  }
  if ((c >= 65) && (c <= 70))
    return (c - 55);
  else {
  }
  if ((c >= 97) && (c <= 102))
    return (c - 87);
  else {
  }
  return (0 - 1);
}
int is_hex_digit(int c) {
  if (hex_digit_value(c) >= 0)
    return 1;
  else {
  }
  return 0;
}
int is_binary_digit(int c) {
  if ((c == 48) || (c == 49))
    return 1;
  else {
  }
  return 0;
}
int is_alpha(int c) {
  if (c > 64) {
    if (c < 91)
      return 1;
    else {
    }
  } else {
  }
  if (c > 96) {
    if (c < 123)
      return 1;
    else {
    }
  } else {
  }
  if (c == 95)
    return 1;
  else {
  }
  return 0;
}
int is_alnum(int c) {
  if (is_alpha(c) == 1)
    return 1;
  else {
  }
  return is_digit(c);
}
int source_peek(void) {
  if (source_pos < source_len)
    return source[source_pos];
  else {
  }
  return 0;
}
int source_take(void) {
  int c = source_peek();
  if (source_pos < source_len)
    source_pos = (source_pos + 1);
  else {
  }
  current_source_pos = source_pos;
  return c;
}
int span_hash(int start, int length) {
  int i = 0;
  int h = 7;
  while (i < length) {
    h = ((h * 31) + source[(start + i)]);
    if (h > 1000000)
      h = (h - ((h / 1000000) * 1000000));
    else {
    }
    i = (i + 1);
  }
  return h;
}
int span_equal(int a, int b, int length) {
  int i = 0;
  while (i < length) {
    if (source[(a + i)] != source[(b + i)])
      return 0;
    else {
    }
    i = (i + 1);
  }
  return 1;
}
int sym_lookup(int start, int length, int h) {
  int i = 1;
  while (i < sym_count) {
    if (sym_len[i] == length) {
      if (sym_hash[i] == h) {
        if (span_equal(sym_start[i], start, length) == 1)
          return i;
        else {
        }
      } else {
      }
    } else {
    }
    i = (i + 1);
  }
  return 0;
}
int sym_tag_id(void) {
  if (sym_tag_name != 0)
    return sym_tag_name;
  else {
  }
  int start = (source_len + sym_text_len);
  (void)(ensure_source((start + 2)));
  source[start] = 116;
  source[(start + 1)] = 97;
  source[(start + 2)] = 103;
  sym_tag_name = sym_intern(start, 3, L_ID, 0);
  sym_text_len = (sym_text_len + 3);
  return sym_tag_name;
}
int sym_qualified(int ns, int name) {
  if (ns == 0)
    return name;
  else {
  }
  int start = (source_len + sym_text_len);
  int out = 0;
  int i = 0;
  while (i < sym_len[ns]) {
    (void)(ensure_source((start + out)));
    source[(start + out)] = source[(sym_start[ns] + i)];
    out = (out + 1);
    i = (i + 1);
  }
  (void)(ensure_source((start + out)));
  source[(start + out)] = 58;
  out = (out + 1);
  (void)(ensure_source((start + out)));
  source[(start + out)] = 58;
  out = (out + 1);
  i = 0;
  while (i < sym_len[name]) {
    (void)(ensure_source((start + out)));
    source[(start + out)] = source[(sym_start[name] + i)];
    out = (out + 1);
    i = (i + 1);
  }
  int id = sym_intern(start, out, L_ID, 0);
  sym_text_len = (sym_text_len + out);
  return id;
}
int ast_decl_name(int name) {
  if (ast_namespace_scope == 0)
    return name;
  else {
  }
  return sym_qualified(ast_namespace_scope, name);
}
int ast_type_name(int name) {
  return name;
}
int ast_is_match_default(int name) {
  if (((name != 0) && (sym_len[name] == 1)) && (source[sym_start[name]] == 95))
    return 1;
  else {
  }
  return 0;
}
int sym_intern(int start, int length, int kind, int scope) {
  int h = span_hash(start, length);
  int old = sym_lookup(start, length, h);
  if (old != 0)
    return old;
  else {
  }
  int id = sym_count;
  (void)(ensure_sym(id));
  sym_start[id] = start;
  sym_len[id] = length;
  sym_hash[id] = h;
  sym_kind[id] = kind;
  sym_scope[id] = scope;
  sym_type[id] = 0;
  sym_elem_kind[id] = 0;
  sym_elem_name[id] = 0;
  sym_count = (sym_count + 1);
  return id;
}
void ensure_bi(int need) {
  if (need < bi_cap)
    return;
  else {
  }
  int n = next_capacity(bi_cap, need);
  bi_name = grow_ints(bi_name, bi_cap, n);
  bi_len = grow_ints(bi_len, bi_cap, n);
  bi_tc = grow_ints(bi_tc, bi_cap, n);
  bi_flags = grow_ints(bi_flags, bi_cap, n);
  bi_cap = n;
}
void bi_register(char *text, int tc_tag, int flags) {
  int len = 0;
  while (text[len] != 0) {
    len = (len + 1);
  }
  int start = (source_len + sym_text_len);
  int i = 0;
  while (i < len) {
    (void)(ensure_source((start + i)));
    source[(start + i)] = text[i];
    i = (i + 1);
  }
  sym_text_len = (sym_text_len + len);
  int id = sym_intern(start, len, L_STRING, 0);
  (void)(ensure_bi(bi_count));
  bi_name[bi_count] = id;
  bi_len[bi_count] = len;
  bi_tc[bi_count] = tc_tag;
  bi_flags[bi_count] = flags;
  bi_count = (bi_count + 1);
}
int bi_lookup(int name) {
  if (bi_count == 0)
    (void)(bi_init());
  else {
  }
  int i = 0;
  while (i < bi_count) {
    if ((sym_len[name] == bi_len[i]) && (sym_hash[name] == sym_hash[bi_name[i]]))
      return i;
    else {
    }
    i = (i + 1);
  }
  return (0 - 1);
}
int bi_tag(int name) {
  int i = bi_lookup(name);
  if (i < 0)
    return BI_TC_NONE;
  else {
  }
  return bi_tc[i];
}
int bi_has_flag(int name, int flag) {
  int i = bi_lookup(name);
  if (i < 0)
    return 0;
  else {
  }
  if ((bi_flags[i] & flag) != 0)
    return 1;
  else {
  }
  return 0;
}
void bi_init(void) {
  (void)(bi_register("printf", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("runtime_read_line", BI_TC_READ_LINE, (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("runtime_read_int", BI_TC_READ_INT, BI_FLAG_RESERVED));
  (void)(bi_register("runtime_write_string", BI_TC_WRITE_STRING, BI_FLAG_RESERVED));
  (void)(bi_register("runtime_write_line", BI_TC_WRITE_LINE, BI_FLAG_RESERVED));
  (void)(bi_register("runtime_write_int", BI_TC_WRITE_INT, BI_FLAG_RESERVED));
  (void)(bi_register("runtime_write_char", BI_TC_WRITE_CHAR, BI_FLAG_RESERVED));
  (void)(bi_register("runtime_io_status", BI_TC_IO_STATUS, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_sys_run", BI_TC_SYS_RUN, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_compile_argv", BI_TC_SYS_COMPILE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_sys_stdout", BI_TC_SYS_STRING, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_sys_stderr", BI_TC_SYS_STRING, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_sys_truncated", BI_TC_SYS_INT, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_sys_spawn_error", BI_TC_SYS_INT, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_sys_run_status", BI_TC_SYS_INT, BI_FLAG_RESERVED));
  (void)(bi_register("memory_alloc", BI_TC_MEM_ALLOC, (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("memory_alloc_aligned", BI_TC_MEM_ALLOC_ALIGNED,
                     (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("memory_resize", BI_TC_MEM_RESIZE, (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("memory_free", BI_TC_MEM_FREE, (BI_FLAG_RESERVED + BI_FLAG_CONSUME)));
  (void)(bi_register("alloc_ints", BI_TC_PTR_INT, (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("free_ints", BI_TC_VOID, (BI_FLAG_RESERVED + BI_FLAG_CONSUME)));
  (void)(bi_register("grow_ints", BI_TC_PTR_INT, BI_FLAG_RESERVED));
  (void)(bi_register("open_file", BI_TC_PTR_VOID, (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("read_char", BI_TC_INT, BI_FLAG_RESERVED));
  (void)(bi_register("close_file", BI_TC_INT, (BI_FLAG_RESERVED + BI_FLAG_CONSUME)));
  (void)(bi_register("write_char", BI_TC_INT, BI_FLAG_RESERVED));
  (void)(bi_register("write_string", BI_TC_INT, BI_FLAG_RESERVED));
  (void)(bi_register("write_int", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("runtime_string_concat", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_track", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_release", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_memory_alloc", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_memory_resize", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_memory_free", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_panic", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_checked_bytes", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_find", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_validate", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_cleanup", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_inc_find", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_inc_add", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_inc_strdup", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_inc_realpath", BI_TC_STRING, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_inc_join", BI_TC_STRING, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_include_line_mode", BI_TC_INT, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_include_close", BI_TC_VOID, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_include_open_root", BI_TC_PTR_INT,
                     (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("basalt_include_open_line", BI_TC_PTR_INT,
                     (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("basalt_include_last_status", BI_TC_INT, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_include_reset_session", BI_TC_VOID, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_atomic_make", BI_TC_ATOMIC_MAKE, (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("basalt_atomic_load", BI_TC_ATOMIC_LOAD, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_atomic_store", BI_TC_ATOMIC_STORE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_atomic_fetch_add", BI_TC_ATOMIC_FETCH_ADD, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_atomic_compare_exchange", BI_TC_ATOMIC_CAS, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_atomic_free", BI_TC_ATOMIC_FREE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_channel_make", BI_TC_CHANNEL_MAKE,
                     (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("basalt_channel_send", BI_TC_CHANNEL_SEND, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_channel_recv", BI_TC_CHANNEL_RECV, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_channel_close", BI_TC_CHANNEL_CLOSE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_channel_free", BI_TC_CHANNEL_FREE, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_thread_spawn", BI_TC_THREAD_SPAWN,
                     (BI_FLAG_RESERVED + BI_FLAG_OWNED)));
  (void)(bi_register("basalt_thread_join", BI_TC_THREAD_JOIN, BI_FLAG_RESERVED));
  (void)(bi_register("basalt_thread_yield", BI_TC_THREAD_YIELD, BI_FLAG_RESERVED));
  (void)(bi_register("malloc", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("calloc", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("realloc", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("free", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("memcpy", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("memset", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("strlen", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("strrchr", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("fopen", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("fclose", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("fgetc", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("fputc", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("fputs", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("fprintf", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("exit", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("atexit", BI_TC_NONE, BI_FLAG_RESERVED));
  (void)(bi_register("len", BI_TC_NONE, BI_FLAG_DYNFIELD));
  (void)(bi_register("cap", BI_TC_NONE, BI_FLAG_DYNFIELD));
  (void)(bi_register("main", BI_TC_NONE, BI_FLAG_MAIN));
}
int word_code(int start, int length) {
  int h = span_hash(start, length);
  if (length == 2) {
    if (h == 10084)
      return L_IF;
    else {
    }
    if (h == 9999)
      return L_FN;
    else {
    }
    if ((source[start] == 117) && (source[(start + 1)] == 56))
      return L_TU8;
    else {
    }
    if ((source[start] == 105) && (source[(start + 1)] == 56))
      return L_TI8;
    else {
    }
  } else {
  }
  if (length == 3) {
    if (((source[start] == 109) && (source[(start + 1)] == 117)) && (source[(start + 2)] == 116))
      return L_MUT;
    else {
    }
    if (h == 315572)
      return L_LET;
    else {
    }
    if (h == 312968)
      return L_TINT;
    else {
    }
    if (h == 310114)
      return L_FOR;
    else {
    }
    if (((source[start] == 117) && (source[(start + 1)] == 49)) && (source[(start + 2)] == 54))
      return L_TU16;
    else {
    }
    if (((source[start] == 102) && (source[(start + 1)] == 51)) && (source[(start + 2)] == 50))
      return L_TFLOAT;
    else {
    }
    if (((source[start] == 102) && (source[(start + 1)] == 54)) && (source[(start + 2)] == 52))
      return L_TDOUBLE;
    else {
    }
    if (((source[start] == 117) && (source[(start + 1)] == 51)) && (source[(start + 2)] == 50))
      return L_TU32;
    else {
    }
    if (((source[start] == 117) && (source[(start + 1)] == 54)) && (source[(start + 2)] == 52))
      return L_TU64;
    else {
    }
    if (((source[start] == 105) && (source[(start + 1)] == 49)) && (source[(start + 2)] == 54))
      return L_TI16;
    else {
    }
    if (((source[start] == 105) && (source[(start + 1)] == 51)) && (source[(start + 2)] == 50))
      return L_TI32;
    else {
    }
    if (((source[start] == 105) && (source[(start + 1)] == 54)) && (source[(start + 2)] == 52))
      return L_TI64;
    else {
    }
  } else {
  }
  if (length == 4) {
    if (h == 619275)
      return L_FUNC;
    else {
    }
    if (h == 580992)
      return L_ELSE;
    else {
    }
    if (h == 582984)
      return L_ENUM;
    else {
    }
    if (h == 33685)
      return L_TRUE;
    else {
    }
    if (h == 494385)
      return L_TBOOL;
    else {
    }
    if (h == 90011)
      return L_TVOID;
    else {
    }
    if (h == 23588)
      return L_THEN;
    else {
    }
    if ((((source[start] == 109) && (source[(start + 1)] == 111)) &&
         (source[(start + 2)] == 118)) &&
        (source[(start + 3)] == 101))
      return L_MOVE;
    else {
    }
  } else {
  }
  if (length == 5) {
    if (h == 339014)
      return L_PRINT;
    else {
    }
    if (h == 505674)
      return L_WHILE;
    else {
    }
    if (h == 600380)
      return L_FALSE;
    else {
    }
    if (h == 405464)
      return L_BREAK;
    else {
    }
    if (((((source[start] == 100) && (source[(start + 1)] == 101)) &&
          (source[(start + 2)] == 102)) &&
         (source[(start + 3)] == 101)) &&
        (source[(start + 4)] == 114))
      return L_DEFER;
    else {
    }
    if (((((source[start] == 109) && (source[(start + 1)] == 97)) &&
          (source[(start + 2)] == 116)) &&
         (source[(start + 3)] == 99)) &&
        (source[(start + 4)] == 104))
      return L_MATCH;
    else {
    }
  } else {
  }
  if (length == 7) {
    if (((((((source[start] == 112) && (source[(start + 1)] == 114)) &&
            (source[(start + 2)] == 105)) &&
           (source[(start + 3)] == 110)) &&
          (source[(start + 4)] == 116)) &&
         (source[(start + 5)] == 108)) &&
        (source[(start + 6)] == 110))
      return L_PRINTLN;
    else {
    }
  } else {
  }
  if (length == 8) {
    if (source[start] == 99) {
      if (source[(start + 1)] == 111) {
        if (source[(start + 2)] == 110) {
          if (source[(start + 3)] == 116) {
            if (source[(start + 4)] == 105) {
              if (source[(start + 5)] == 110) {
                if (source[(start + 6)] == 117) {
                  if (source[(start + 7)] == 101)
                    return L_CONTINUE;
                  else {
                  }
                } else {
                }
              } else {
              }
            } else {
            }
          } else {
          }
        } else {
        }
      } else {
      }
    } else {
    }
  } else {
  }
  if (length == 4) {
    if ((((source[start] == 110) && (source[(start + 1)] == 117)) &&
         (source[(start + 2)] == 108)) &&
        (source[(start + 3)] == 108))
      return L_NULL;
    else {
    }
    if ((((source[start] == 99) && (source[(start + 1)] == 104)) && (source[(start + 2)] == 97)) &&
        (source[(start + 3)] == 114))
      return L_TCHAR;
    else {
    }
    if ((((source[start] == 108) && (source[(start + 1)] == 111)) &&
         (source[(start + 2)] == 110)) &&
        (source[(start + 3)] == 103))
      return L_TLONG;
    else {
    }
  } else {
  }
  if (length == 5) {
    if (((((source[start] == 99) && (source[(start + 1)] == 111)) &&
          (source[(start + 2)] == 110)) &&
         (source[(start + 3)] == 115)) &&
        (source[(start + 4)] == 116))
      return L_CONST;
    else {
    }
    if (((((source[start] == 97) && (source[(start + 1)] == 114)) &&
          (source[(start + 2)] == 114)) &&
         (source[(start + 3)] == 97)) &&
        (source[(start + 4)] == 121))
      return L_ARRAY;
    else {
    }
  } else {
  }
  if (length == 6) {
    if ((((((source[start] == 98) && (source[(start + 1)] == 111)) &&
           (source[(start + 2)] == 114)) &&
          (source[(start + 3)] == 114)) &&
         (source[(start + 4)] == 111)) &&
        (source[(start + 5)] == 119))
      return L_BORROW;
    else {
    }
    if (h == 448999)
      return L_EXTERN;
    else {
    }
    if (source[start] == 114) {
      if (source[(start + 1)] == 101) {
        if (source[(start + 2)] == 116) {
          if (source[(start + 3)] == 117) {
            if (source[(start + 4)] == 114) {
              if (source[(start + 5)] == 110)
                return L_RETURN;
              else {
              }
            } else {
            }
          } else {
          }
        } else {
        }
      } else {
      }
    } else {
    }
    if (source[start] == 115) {
      if (source[(start + 1)] == 116) {
        if (source[(start + 2)] == 114) {
          if (source[(start + 3)] == 117) {
            if (source[(start + 4)] == 99) {
              if (source[(start + 5)] == 116)
                return L_STRUCT;
              else {
              }
            } else {
            }
          } else {
          }
        } else {
        }
      } else {
      }
    } else {
    }
    if (source[start] == 115) {
      if (source[(start + 1)] == 116) {
        if (source[(start + 2)] == 114) {
          if (source[(start + 3)] == 105) {
            if (source[(start + 4)] == 110) {
              if (source[(start + 5)] == 103)
                return L_TSTRING;
              else {
              }
            } else {
            }
          } else {
          }
        } else {
        }
      } else {
      }
    } else {
    }
  } else {
  }
  if (length == 5) {
    if (((((source[start] == 102) && (source[(start + 1)] == 108)) &&
          (source[(start + 2)] == 111)) &&
         (source[(start + 3)] == 97)) &&
        (source[(start + 4)] == 116))
      return L_TFLOAT;
    else {
    }
    if (((((source[start] == 117) && (source[(start + 1)] == 115)) &&
          (source[(start + 2)] == 105)) &&
         (source[(start + 3)] == 122)) &&
        (source[(start + 4)] == 101))
      return L_TUSIZE;
    else {
    }
  } else {
  }
  if (length == 7) {
    if (((((((source[start] == 97) && (source[(start + 1)] == 108)) &&
            (source[(start + 2)] == 105)) &&
           (source[(start + 3)] == 103)) &&
          (source[(start + 4)] == 110)) &&
         (source[(start + 5)] == 97)) &&
        (source[(start + 6)] == 115))
      return L_ALIGNAS;
    else {
    }
    if (((((((source[start] == 99) && (source[(start + 1)] == 108)) &&
            (source[(start + 2)] == 111)) &&
           (source[(start + 3)] == 115)) &&
          (source[(start + 4)] == 117)) &&
         (source[(start + 5)] == 114)) &&
        (source[(start + 6)] == 101))
      return L_CLOSURE;
    else {
    }
  } else {
  }
  if (length == 9) {
    if (((((((((source[start] == 110) && (source[(start + 1)] == 97)) &&
              (source[(start + 2)] == 109)) &&
             (source[(start + 3)] == 101)) &&
            (source[(start + 4)] == 115)) &&
           (source[(start + 5)] == 112)) &&
          (source[(start + 6)] == 97)) &&
         (source[(start + 7)] == 99)) &&
        (source[(start + 8)] == 101))
      return L_NAMESPACE;
    else {
    }
  } else {
  }
  if (length == 10) {
    if ((((((((((source[start] == 98) && (source[(start + 1)] == 111)) &&
               (source[(start + 2)] == 114)) &&
              (source[(start + 3)] == 114)) &&
             (source[(start + 4)] == 111)) &&
            (source[(start + 5)] == 119)) &&
           (source[(start + 6)] == 95)) &&
          (source[(start + 7)] == 109)) &&
         (source[(start + 8)] == 117)) &&
        (source[(start + 9)] == 116))
      return L_BORROW_MUT;
    else {
    }
  } else {
  }
  if (length == 6) {
    if ((((((source[start] == 100) && (source[(start + 1)] == 111)) &&
           (source[(start + 2)] == 117)) &&
          (source[(start + 3)] == 98)) &&
         (source[(start + 4)] == 108)) &&
        (source[(start + 5)] == 101))
      return L_TDOUBLE;
    else {
    }
  } else {
  }
  return L_ID;
}
void ensure_lexer_literal_digits(int need) {
  if (need < lexer_literal_digits_cap)
    return;
  else {
  }
  int n = next_capacity(lexer_literal_digits_cap, need);
  lexer_literal_digits = grow_ints(lexer_literal_digits, lexer_literal_digits_cap, n);
  lexer_literal_digits_cap = n;
}
int lexer_make_integer(int start, int length, int base) {
  (void)(ensure_lexer_literal_digits(32));
  int digit_count = 1;
  int overflow_u64 = 0;
  int p = start;
  int end = (start + length);
  int prefix = 0;
  if ((base == 16) || (base == 2))
    prefix = 2;
  else {
  }
  p = (start + prefix);
  lexer_literal_digits[0] = 0;
  while (p < end) {
    int digit = (0 - 1);
    if (base == 16)
      digit = hex_digit_value(source[p]);
    else if (base == 2) {
      if ((source[p] == 48) || (source[p] == 49))
        digit = (source[p] - 48);
      else {
      }
    } else if (is_digit(source[p]) == 1)
      digit = (source[p] - 48);
    else {
    }
    if ((digit < 0) || (digit >= base)) {
      lexer_error = 1;
      return 0;
    } else {
    }
    if (overflow_u64 == 0) {
      int i = 0;
      int carry = digit;
      while (i < digit_count) {
        int product = ((lexer_literal_digits[i] * base) + carry);
        lexer_literal_digits[i] = (product % 10);
        carry = (product / 10);
        i = (i + 1);
      }
      if (carry != 0) {
        if (digit_count >= 20)
          overflow_u64 = 1;
        else {
          lexer_literal_digits[digit_count] = carry;
          digit_count = (digit_count + 1);
        }
      } else {
      }
    } else {
    }
    p = (p + 1);
  }
  if (overflow_u64 == 1) {
    lexer_error = 1;
    return 0;
  } else {
  }
  int decimal_start = (source_len + sym_text_len);
  int i = (digit_count - 1);
  int out = 0;
  while (i >= 0) {
    (void)(ensure_source((decimal_start + out)));
    source[(decimal_start + out)] = (48 + lexer_literal_digits[i]);
    out = (out + 1);
    i = (i - 1);
  }
  int text_id = sym_intern(decimal_start, out, L_INT, 0);
  sym_text_len = (sym_text_len + out);
  int value = 0;
  int overflow_int = 0;
  i = (digit_count - 1);
  while (i >= 0) {
    int digit = lexer_literal_digits[i];
    if (overflow_int == 0) {
      if (value > 214748364)
        overflow_int = 1;
      else if ((value == 214748364) && (digit > 7))
        overflow_int = 1;
      else
        value = ((value * 10) + digit);
    } else {
    }
    i = (i - 1);
  }
  tok_kind = L_INT;
  tok_value = value;
  if (overflow_int == 1)
    tok_value = (0 - 1);
  else {
  }
  tok_text = text_id;
  tok_length = length;
  return 1;
}
void lexer_skip(void) {
  while (1 == 1) {
    while (is_space(source_peek()) == 1) {
      (void)(source_take());
    }
    if ((source_peek() != 47) || ((source_pos + 1) >= source_len))
      return;
    else {
    }
    if (source[(source_pos + 1)] == 47) {
      (void)(source_take());
      (void)(source_take());
      while (source_peek() != 10) {
        if (source_pos >= source_len)
          return;
        else {
        }
        (void)(source_take());
      }
    } else if (source[(source_pos + 1)] == 42) {
      (void)(source_take());
      (void)(source_take());
      int closed = 0;
      while (source_pos < source_len) {
        if (((source_peek() == 42) && ((source_pos + 1) < source_len)) &&
            (source[(source_pos + 1)] == 47)) {
          (void)(source_take());
          (void)(source_take());
          closed = 1;
          { break; }
        } else {
        }
        (void)(source_take());
      }
      if (closed == 0) {
        lexer_error = 1;
        current_source_pos = source_pos;
        return;
      } else {
      }
    } else
      return;
  }
}
int lexer_next(void) {
  (void)(lexer_skip());
  if (lexer_error == 1) {
    tok_kind = L_EOF;
    tok_value = 0;
    tok_text = 0;
    tok_length = 0;
    return tok_kind;
  } else {
  }
  tok_start = source_pos;
  tok_length = 0;
  tok_text = 0;
  int c = source_peek();
  if (c == 0) {
    tok_kind = L_EOF;
    tok_value = 0;
    return tok_kind;
  } else {
  }
  if (is_alpha(c) == 1) {
    while (is_alnum(source_peek()) == 1) {
      (void)(source_take());
    }
    tok_length = (source_pos - tok_start);
    tok_kind = word_code(tok_start, tok_length);
    if ((tok_kind == L_ID) || (tok_kind == L_ARRAY))
      tok_value = sym_intern(tok_start, tok_length, tok_kind, 0);
    else
      tok_value = 0;
    return tok_kind;
  } else {
  }
  if (c == 39) {
    (void)(source_take());
    int v = source_take();
    if (v == 92) {
      int e = source_take();
      if (e == 110)
        v = 10;
      else if (e == 116)
        v = 9;
      else if (e == 114)
        v = 13;
      else if (e == 98)
        v = 8;
      else if (e == 102)
        v = 12;
      else if (e == 118)
        v = 11;
      else if (e == 48)
        v = 0;
      else
        v = e;
    } else {
    }
    if (source_peek() != 39) {
      tok_kind = L_EOF;
      tok_value = 0;
      return tok_kind;
    } else {
    }
    (void)(source_take());
    tok_kind = L_CHAR;
    tok_value = v;
    tok_length = 3;
    return tok_kind;
  } else {
  }
  if (c == 34) {
    (void)(source_take());
    while (source_peek() != 34) {
      if (source_pos > (source_len - 1)) {
        tok_kind = L_EOF;
        tok_value = 0;
        return tok_kind;
      } else {
      }
      if (source_peek() == 92) {
        (void)(source_take());
        if (source_peek() != 0)
          (void)(source_take());
        else {
        }
      } else
        (void)(source_take());
    }
    (void)(source_take());
    tok_kind = L_STRING;
    tok_length = (source_pos - tok_start);
    tok_value = sym_intern((tok_start + 1), (tok_length - 2), L_STRING, 0);
    return tok_kind;
  } else {
  }
  if (is_digit(c) == 1) {
    if (((c == 48) && ((source_pos + 1) < source_len)) &&
        ((source[(source_pos + 1)] == 120) || (source[(source_pos + 1)] == 88))) {
      (void)(source_take());
      (void)(source_take());
      int hex_start = tok_start;
      while (is_hex_digit(source_peek()) == 1) {
        (void)(source_take());
      }
      if (source_pos == (tok_start + 2)) {
        lexer_error = 1;
        return L_EOF;
      } else {
      }
      tok_length = (source_pos - tok_start);
      (void)(lexer_make_integer(hex_start, tok_length, 16));
      return tok_kind;
    } else {
    }
    if (((c == 48) && ((source_pos + 1) < source_len)) &&
        ((source[(source_pos + 1)] == 98) || (source[(source_pos + 1)] == 66))) {
      (void)(source_take());
      (void)(source_take());
      int binary_start = tok_start;
      while (is_binary_digit(source_peek()) == 1) {
        (void)(source_take());
      }
      if (source_pos == (tok_start + 2)) {
        lexer_error = 1;
        return L_EOF;
      } else {
      }
      tok_length = (source_pos - tok_start);
      (void)(lexer_make_integer(binary_start, tok_length, 2));
      return tok_kind;
    } else {
    }
    int value = 0;
    int overflow = 0;
    while (is_digit(source_peek()) == 1) {
      int digit = (source_peek() - 48);
      (void)(source_take());
      if (overflow == 0) {
        if (value > 214748364)
          overflow = 1;
        else if ((value == 214748364) && (digit > 7))
          overflow = 1;
        else
          value = ((value * 10) + digit);
      } else {
      }
    }
    int is_float = 0;
    if (source_peek() == 46) {
      is_float = 1;
      (void)(source_take());
      while (is_digit(source_peek()) == 1) {
        (void)(source_take());
      }
    } else {
    }
    if ((source_peek() == 101) || (source_peek() == 69)) {
      is_float = 1;
      (void)(source_take());
      if ((source_peek() == 43) || (source_peek() == 45))
        (void)(source_take());
      else {
      }
      if (is_digit(source_peek()) == 0) {
        lexer_error = 1;
        return L_EOF;
      } else {
      }
      while (is_digit(source_peek()) == 1) {
        (void)(source_take());
      }
    } else {
    }
    if (is_float == 1) {
      tok_kind = L_FLOAT;
      tok_length = (source_pos - tok_start);
      tok_text = 0;
      tok_value = sym_intern(tok_start, tok_length, L_FLOAT, 0);
      return tok_kind;
    } else {
    }
    tok_kind = L_INT;
    tok_length = (source_pos - tok_start);
    tok_text = sym_intern(tok_start, tok_length, L_INT, 0);
    if (overflow == 1)
      tok_value = (0 - 1);
    else
      tok_value = value;
    return tok_kind;
  } else {
  }
  (void)(source_take());
  tok_length = 1;
  if (c == 43) {
    if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_PLUS_EQ;
    } else if (source_peek() == 43) {
      (void)(source_take());
      tok_kind = L_CONCAT;
    } else
      tok_kind = L_PLUS;
  } else if (c == 45) {
    if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_MINUS_EQ;
    } else
      tok_kind = L_MINUS;
  } else if (c == 42) {
    if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_STAR_EQ;
    } else
      tok_kind = L_STAR;
  } else if (c == 47) {
    if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_DIV_EQ;
    } else
      tok_kind = L_DIV;
  } else if (c == 37) {
    if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_MOD_EQ;
    } else
      tok_kind = L_MOD;
  } else if (c == 40)
    tok_kind = L_LPAREN;
  else if (c == 41)
    tok_kind = L_RPAREN;
  else if (c == 123)
    tok_kind = L_LBRACE;
  else if (c == 125)
    tok_kind = L_RBRACE;
  else if (c == 58) {
    if (source_peek() == 58) {
      (void)(source_take());
      tok_kind = L_SCOPE;
    } else
      tok_kind = L_COLON;
  } else if (c == 59)
    tok_kind = L_SEMI;
  else if (c == 44)
    tok_kind = L_COMMA;
  else if (c == 91)
    tok_kind = L_LBRACK;
  else if (c == 93)
    tok_kind = L_RBRACK;
  else if (c == 46)
    tok_kind = L_DOT;
  else if (c == 60) {
    if (source_peek() == 60) {
      (void)(source_take());
      if (source_peek() == 61) {
        (void)(source_take());
        tok_kind = L_SHL_EQ;
      } else
        tok_kind = L_SHL;
    } else if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_LE;
    } else
      tok_kind = L_LT;
  } else if (c == 62) {
    if (source_peek() == 62) {
      (void)(source_take());
      if (source_peek() == 61) {
        (void)(source_take());
        tok_kind = L_SHR_EQ;
      } else
        tok_kind = L_SHR;
    } else if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_GE;
    } else
      tok_kind = L_GT;
  } else if (c == 124) {
    if (source_peek() == 124) {
      (void)(source_take());
      tok_kind = L_OR;
    } else if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_BITOR_EQ;
    } else
      tok_kind = L_BITOR;
  } else if (c == 94) {
    if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_BITXOR_EQ;
    } else
      tok_kind = L_BITXOR;
  } else if (c == 126)
    tok_kind = L_BITNOT;
  else if (c == 61) {
    if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_EQEQ;
    } else if (source_peek() == 62) {
      (void)(source_take());
      tok_kind = L_FATARROW;
    } else
      tok_kind = L_EQ;
  } else if (c == 33) {
    if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_NEQ;
    } else
      tok_kind = L_NOT;
  } else if (c == 38) {
    if (source_peek() == 38) {
      (void)(source_take());
      tok_kind = L_AND;
    } else if (source_peek() == 61) {
      (void)(source_take());
      tok_kind = L_AMP_EQ;
    } else
      tok_kind = L_AMP;
  } else if (c == 124) {
    if (source_peek() == 124) {
      (void)(source_take());
      tok_kind = L_OR;
    } else
      tok_kind = L_EOF;
  } else
    tok_kind = L_EOF;
  tok_value = 0;
  return tok_kind;
}
void include_process_line(int *line, int length) {
  int mode = 0;
  int p = 0;
  while ((p < length) && ((line[p] == 32) || (line[p] == 9))) {
    p = (p + 1);
  }
  if ((p + 7) < length) {
    if (((((((line[p] == 105) && (line[(p + 1)] == 110)) && (line[(p + 2)] == 99)) &&
           (line[(p + 3)] == 108)) &&
          (line[(p + 4)] == 117)) &&
         (line[(p + 5)] == 100)) &&
        (line[(p + 6)] == 101)) {
      if ((line[(p + 7)] == 32) || (line[(p + 7)] == 9))
        mode = 1;
      else {
      }
    } else {
    }
  } else {
  }
  if (((p + 8) < length) && (mode == 0)) {
    if ((((((((line[p] == 105) && (line[(p + 1)] == 110)) && (line[(p + 2)] == 99)) &&
            (line[(p + 3)] == 108)) &&
           (line[(p + 4)] == 117)) &&
          (line[(p + 5)] == 100)) &&
         (line[(p + 6)] == 101)) &&
        (line[(p + 7)] == 99)) {
      if ((line[(p + 8)] == 32) || (line[(p + 8)] == 9))
        mode = 2;
      else {
      }
    } else {
    }
  } else {
  }
  if (mode == 0) {
    int i = 0;
    while (i < length) {
      (void)(source_put(line[i]));
      i = (i + 1);
    }
    (void)(source_put(10));
  } else if (mode == 1) {
    int parent_file = source_active_file;
    int parent_line = source_active_line;
    int *child_module = basalt_include_open_line(line, length, mode);
    int status_module = basalt_include_last_status();
    char *target_path = basalt_inc_realpath("");
    int target_file = 0;
    if (target_path[0] != 0)
      target_file = source_file_intern(target_path);
    else {
    }
    if (target_file == 0) {
      (void)(source_import_fail(3, parent_file, 0, parent_line));
    } else {
      (void)(source_dep_add(parent_file, target_file));
      if (source_dep_reaches(target_file, parent_file) == 1)
        (void)(source_import_fail(1, parent_file, target_file, parent_line));
      else if (child_module == 0) {
        if (status_module == 2) {
        } else if (status_module == 1)
          (void)(source_import_fail(1, parent_file, target_file, parent_line));
        else if (status_module == 3)
          (void)(source_import_fail(2, parent_file, target_file, parent_line));
        else
          (void)(source_import_fail(3, parent_file, target_file, parent_line));
      } else {
        source_active_file = target_file;
        source_active_line = 1;
        (void)(include_expand_handle(child_module));
        source_active_file = parent_file;
        source_active_line = parent_line;
        (void)(close_file(child_module));
        (void)(basalt_include_close());
      }
    }
  } else {
    int parent_file = source_active_file;
    int parent_line = source_active_line;
    int *child_c = basalt_include_open_line(line, length, mode);
    int status_c = basalt_include_last_status();
    char *target_path_c = basalt_inc_realpath("");
    int target_file_c = 0;
    if (target_path_c[0] != 0)
      target_file_c = source_file_intern(target_path_c);
    else {
    }
    if (target_file_c == 0) {
      (void)(source_import_fail(3, parent_file, 0, parent_line));
    } else {
      (void)(source_dep_add(parent_file, target_file_c));
      if (source_dep_reaches(target_file_c, parent_file) == 1)
        (void)(source_import_fail(1, parent_file, target_file_c, parent_line));
      else if (child_c == 0) {
        if (status_c == 2) {
        } else if (status_c == 1)
          (void)(source_import_fail(1, parent_file, target_file_c, parent_line));
        else if (status_c == 3)
          (void)(source_import_fail(2, parent_file, target_file_c, parent_line));
        else
          (void)(source_import_fail(3, parent_file, target_file_c, parent_line));
      } else {
        int c = read_char(child_c);
        while (c != (0 - 1)) {
          (void)(c_source_put(c));
          c = read_char(child_c);
        }
        (void)(close_file(child_c));
        (void)(basalt_include_close());
      }
    }
  }
}
void include_expand_handle(int *handle) {
  int line_capacity = include_line_initial_cap;
  int *line = alloc_ints(line_capacity);
  int length = 0;
  int line_number = 1;
  int c = read_char(handle);
  while (c != (0 - 1)) {
    if (c == 10) {
      (void)(include_process_line(line, length));
      length = 0;
      line_number = (line_number + 1);
      source_active_line = line_number;
    } else {
      if (length >= line_capacity) {
        int next_line_capacity = next_capacity(line_capacity, length);
        line = grow_ints(line, line_capacity, next_line_capacity);
        line_capacity = next_line_capacity;
      } else {
      }
      line[length] = c;
      length = (length + 1);
    }
    c = read_char(handle);
  }
  if (length > 0)
    (void)(include_process_line(line, length));
  else {
  }
  (void)(free_ints(line));
}
void load_source_file(char *path) {
  (void)(source_reset());
  int *handle = open_file(path, "r");
  int c = read_char(handle);
  while (c != (0 - 1)) {
    (void)(source_put(c));
    c = read_char(handle);
  }
  (void)(close_file(handle));
}
int map_token(int k) {
  if (k == L_EOF)
    return T_EOF;
  else {
  }
  if (k == L_ID)
    return T_ID;
  else {
  }
  if (k == L_INT)
    return T_INT;
  else {
  }
  if (k == L_STRING)
    return T_STRING;
  else {
  }
  if (k == L_FUNC)
    return T_FUNC;
  else {
  }
  if (k == L_EXTERN)
    return T_EXTERN;
  else {
  }
  if (k == L_LET)
    return T_LET;
  else {
  }
  if (k == L_PRINT)
    return T_PRINT;
  else {
  }
  if (k == L_PRINTLN)
    return T_PRINTLN;
  else {
  }
  if (k == L_RETURN)
    return T_RETURN;
  else {
  }
  if (k == L_DEFER)
    return T_DEFER;
  else {
  }
  if (k == L_MATCH)
    return T_MATCH;
  else {
  }
  if (k == L_FATARROW)
    return T_FATARROW;
  else {
  }
  if (k == L_IF)
    return T_IF;
  else {
  }
  if (k == L_ELSE)
    return T_ELSE;
  else {
  }
  if (k == L_WHILE)
    return T_WHILE;
  else {
  }
  if (k == L_FOR)
    return T_FOR;
  else {
  }
  if (k == L_STRUCT)
    return T_STRUCT;
  else {
  }
  if (k == L_ENUM)
    return T_ENUM;
  else {
  }
  if (k == L_BREAK)
    return T_BREAK;
  else {
  }
  if (k == L_CONTINUE)
    return T_CONTINUE;
  else {
  }
  if (k == L_TRUE)
    return T_TRUE;
  else {
  }
  if (k == L_FALSE)
    return T_FALSE;
  else {
  }
  if (k == L_TINT)
    return T_TINT;
  else {
  }
  if (k == L_TBOOL)
    return T_TBOOL;
  else {
  }
  if (k == L_TSTRING)
    return T_TSTRING;
  else {
  }
  if (k == L_TVOID)
    return T_TVOID;
  else {
  }
  if (k == L_THEN)
    return T_THEN;
  else {
  }
  if (k == L_PLUS)
    return T_PLUS;
  else {
  }
  if (k == L_MINUS)
    return T_MINUS;
  else {
  }
  if (k == L_STAR)
    return T_STAR;
  else {
  }
  if (k == L_DIV)
    return T_DIVIDE;
  else {
  }
  if (k == L_MOD)
    return T_MOD;
  else {
  }
  if (k == L_TLONG)
    return T_TLONG;
  else {
  }
  if (k == L_ALIGNAS)
    return T_ALIGNAS;
  else {
  }
  if (k == L_TU8)
    return T_TU8;
  else {
  }
  if (k == L_TU16)
    return T_TU16;
  else {
  }
  if (k == L_TU32)
    return T_TU32;
  else {
  }
  if (k == L_TU64)
    return T_TU64;
  else {
  }
  if (k == L_TI8)
    return T_TI8;
  else {
  }
  if (k == L_TI16)
    return T_TI16;
  else {
  }
  if (k == L_TI32)
    return T_TI32;
  else {
  }
  if (k == L_TI64)
    return T_TI64;
  else {
  }
  if (k == L_TUSIZE)
    return T_TUSIZE;
  else {
  }
  if (k == L_PLUS_EQ)
    return T_PLUS_EQ;
  else {
  }
  if (k == L_MINUS_EQ)
    return T_MINUS_EQ;
  else {
  }
  if (k == L_STAR_EQ)
    return T_STAR_EQ;
  else {
  }
  if (k == L_DIV_EQ)
    return T_DIV_EQ;
  else {
  }
  if (k == L_MOD_EQ)
    return T_MOD_EQ;
  else {
  }
  if (k == L_AMP_EQ)
    return T_AMP_EQ;
  else {
  }
  if (k == L_BITOR_EQ)
    return T_BITOR_EQ;
  else {
  }
  if (k == L_BITXOR_EQ)
    return T_BITXOR_EQ;
  else {
  }
  if (k == L_SHL_EQ)
    return T_SHL_EQ;
  else {
  }
  if (k == L_SHR_EQ)
    return T_SHR_EQ;
  else {
  }
  if (k == L_CONCAT)
    return T_CONCAT;
  else {
  }
  if (k == L_AND)
    return T_AND_AND;
  else {
  }
  if (k == L_OR)
    return T_OR_OR;
  else {
  }
  if (k == L_EQ)
    return T_EQUAL;
  else {
  }
  if (k == L_EQEQ)
    return T_EQEQ;
  else {
  }
  if (k == L_NEQ)
    return T_NEQ;
  else {
  }
  if (k == L_LT)
    return T_LT;
  else {
  }
  if (k == L_GT)
    return T_GT;
  else {
  }
  if (k == L_LE)
    return T_LE;
  else {
  }
  if (k == L_GE)
    return T_GE;
  else {
  }
  if (k == L_NOT)
    return T_NOT;
  else {
  }
  if (k == L_MOVE)
    return T_MOVE;
  else {
  }
  if (k == L_BORROW)
    return T_BORROW;
  else {
  }
  if (k == L_BORROW_MUT)
    return T_BORROW_MUT;
  else {
  }
  if (k == L_CLOSURE)
    return T_CLOSURE;
  else {
  }
  if (k == L_MUT)
    return T_MUT;
  else {
  }
  if (k == L_COLON)
    return T_COLON;
  else {
  }
  if (k == L_LPAREN)
    return T_LPAREN;
  else {
  }
  if (k == L_RPAREN)
    return T_RPAREN;
  else {
  }
  if (k == L_LBRACE)
    return T_LBRACE;
  else {
  }
  if (k == L_RBRACE)
    return T_RBRACE;
  else {
  }
  if (k == L_SEMI)
    return T_SEMI;
  else {
  }
  if (k == L_COMMA)
    return T_COMMA;
  else {
  }
  if (k == L_AMP)
    return T_AMP;
  else {
  }
  if (k == L_LBRACK)
    return T_LBRACK;
  else {
  }
  if (k == L_RBRACK)
    return T_RBRACK;
  else {
  }
  if (k == L_DOT)
    return T_DOT;
  else {
  }
  if (k == L_CHAR)
    return T_CHAR;
  else {
  }
  if (k == L_NULL)
    return T_NULL;
  else {
  }
  if (k == L_CONST)
    return T_CONST;
  else {
  }
  if (k == L_TCHAR)
    return T_TCHAR;
  else {
  }
  if (k == L_FLOAT)
    return T_FLOAT;
  else {
  }
  if (k == L_TFLOAT)
    return T_FLOAT;
  else {
  }
  if (k == L_TDOUBLE)
    return T_TDOUBLE;
  else {
  }
  if (k == L_BITOR)
    return T_BITOR;
  else {
  }
  if (k == L_BITXOR)
    return T_BITXOR;
  else {
  }
  if (k == L_BITNOT)
    return T_BITNOT;
  else {
  }
  if (k == L_SHL)
    return T_SHL;
  else {
  }
  if (k == L_SHR)
    return T_SHR;
  else {
  }
  if (k == L_FN)
    return T_FN;
  else {
  }
  if (k == L_ARRAY)
    return T_ARRAY;
  else {
  }
  if (k == L_NAMESPACE)
    return T_NAMESPACE;
  else {
  }
  if (k == L_SCOPE)
    return T_SCOPE;
  else {
  }
  return T_EOF;
}
void load_tokens_from_file(char *path) {
  (void)(input_reset());
  (void)(basalt_include_reset_session());
  (void)(source_reset());
  sym_text_len = 0;
  sym_count = 1;
  ast_namespace_scope = 0;
  ast_const_count = 0;
  (void)(c_source_reset());
  ffi_header_count = 0;
  lexer_error = 0;
  include_ok = 1;
  source_import_error_kind = 0;
  int *handle = basalt_include_open_root(path);
  int root_id = source_file_intern(basalt_inc_realpath(path));
  if (handle == 0) {
    (void)(source_import_fail(2, 0, root_id, 1));
  } else {
    source_active_file = root_id;
    source_active_line = 1;
    (void)(include_expand_handle(handle));
    (void)(close_file(handle));
    (void)(basalt_include_close());
  }
  int k = lexer_next();
  while (k != L_EOF) {
    if (debug_tokens == 1) {
      (void)(runtime_write_string("token.kind="));
      (void)(runtime_write_int(map_token(k)));
      (void)(runtime_write_char(10));
      (void)(runtime_write_string("token.start="));
      (void)(runtime_write_int(tok_start));
      (void)(runtime_write_char(10));
      (void)(runtime_write_string("token.length="));
      (void)(runtime_write_int(tok_length));
      (void)(runtime_write_char(10));
      (void)(runtime_write_string("token.value="));
      (void)(runtime_write_int(tok_value));
      (void)(runtime_write_char(10));
    } else {
    }
    (void)(input_put(map_token(k), tok_value, tok_text, tok_start));
    k = lexer_next();
  }
  (void)(input_put(T_EOF, 0, 0, source_pos));
}
void ensure_tc_vars(int need) {
  if (need < tc_var_cap)
    return;
  else {
  }
  int n = next_capacity(tc_var_cap, need);
  tc_var_name = grow_ints(tc_var_name, tc_var_cap, n);
  tc_var_kind = grow_ints(tc_var_kind, tc_var_cap, n);
  tc_var_named = grow_ints(tc_var_named, tc_var_cap, n);
  tc_var_elem_kind = grow_ints(tc_var_elem_kind, tc_var_cap, n);
  tc_var_elem_name = grow_ints(tc_var_elem_name, tc_var_cap, n);
  tc_var_type = grow_ints(tc_var_type, tc_var_cap, n);
  tc_var_fun_target = grow_ints(tc_var_fun_target, tc_var_cap, n);
  tc_var_owned = grow_ints(tc_var_owned, tc_var_cap, n);
  tc_var_moved = grow_ints(tc_var_moved, tc_var_cap, n);
  tc_var_borrow_count = grow_ints(tc_var_borrow_count, tc_var_cap, n);
  tc_var_borrow_mut = grow_ints(tc_var_borrow_mut, tc_var_cap, n);
  tc_var_borrow_source = grow_ints(tc_var_borrow_source, tc_var_cap, n);
  tc_var_borrow_mode = grow_ints(tc_var_borrow_mode, tc_var_cap, n);
  tc_var_borrow_parent = grow_ints(tc_var_borrow_parent, tc_var_cap, n);
  tc_var_borrow_param = grow_ints(tc_var_borrow_param, tc_var_cap, n);
  tc_var_param = grow_ints(tc_var_param, tc_var_cap, n);
  tc_var_param_pos = grow_ints(tc_var_param_pos, tc_var_cap, n);
  tc_var_mode = grow_ints(tc_var_mode, tc_var_cap, n);
  tc_var_const = grow_ints(tc_var_const, tc_var_cap, n);
  tc_var_closure_caps = grow_ints(tc_var_closure_caps, tc_var_cap, n);
  tc_var_closure_moved = grow_ints(tc_var_closure_moved, tc_var_cap, n);
  tc_var_ffi_borrowed = grow_ints(tc_var_ffi_borrowed, tc_var_cap, n);
  tc_var_cap = n;
}
void ensure_tc_fun_meta(int need) {
  if (need < tc_fun_meta_cap)
    return;
  else {
  }
  int n = next_capacity(tc_fun_meta_cap, need);
  tc_fun_return_param = grow_ints(tc_fun_return_param, tc_fun_meta_cap, n);
  tc_fun_return_mut = grow_ints(tc_fun_return_mut, tc_fun_meta_cap, n);
  tc_fun_meta_cap = n;
}
void ensure_tc_flow_frames(int need) {
  if (need < tc_flow_frame_cap)
    return;
  else {
  }
  int n = next_capacity(tc_flow_frame_cap, need);
  tc_flow_frame_base = grow_ints(tc_flow_frame_base, tc_flow_frame_cap, n);
  tc_flow_frame_yes = grow_ints(tc_flow_frame_yes, tc_flow_frame_cap, n);
  tc_flow_frame_count = grow_ints(tc_flow_frame_count, tc_flow_frame_cap, n);
  tc_flow_frame_has_yes = grow_ints(tc_flow_frame_has_yes, tc_flow_frame_cap, n);
  tc_flow_frame_field_base = grow_ints(tc_flow_frame_field_base, tc_flow_frame_cap, n);
  tc_flow_frame_field_yes = grow_ints(tc_flow_frame_field_yes, tc_flow_frame_cap, n);
  tc_flow_frame_field_count = grow_ints(tc_flow_frame_field_count, tc_flow_frame_cap, n);
  tc_flow_frame_field_yes_count = grow_ints(tc_flow_frame_field_yes_count, tc_flow_frame_cap, n);
  tc_flow_frame_cap = n;
}
void ensure_tc_flow_log(int need) {
  if (need < tc_flow_log_cap)
    return;
  else {
  }
  int n = next_capacity(tc_flow_log_cap, need);
  tc_flow_log_owned = grow_ints(tc_flow_log_owned, tc_flow_log_cap, n);
  tc_flow_log_moved = grow_ints(tc_flow_log_moved, tc_flow_log_cap, n);
  tc_flow_log_borrow_count = grow_ints(tc_flow_log_borrow_count, tc_flow_log_cap, n);
  tc_flow_log_borrow_mut = grow_ints(tc_flow_log_borrow_mut, tc_flow_log_cap, n);
  tc_flow_log_source = grow_ints(tc_flow_log_source, tc_flow_log_cap, n);
  tc_flow_log_mode = grow_ints(tc_flow_log_mode, tc_flow_log_cap, n);
  tc_flow_log_parent = grow_ints(tc_flow_log_parent, tc_flow_log_cap, n);
  tc_flow_log_borrow_param = grow_ints(tc_flow_log_borrow_param, tc_flow_log_cap, n);
  tc_flow_log_ffi = grow_ints(tc_flow_log_ffi, tc_flow_log_cap, n);
  tc_flow_log_closure_moved = grow_ints(tc_flow_log_closure_moved, tc_flow_log_cap, n);
  tc_flow_log_fun_target = grow_ints(tc_flow_log_fun_target, tc_flow_log_cap, n);
  tc_flow_log_field_root = grow_ints(tc_flow_log_field_root, tc_flow_log_cap, n);
  tc_flow_log_field_name = grow_ints(tc_flow_log_field_name, tc_flow_log_cap, n);
  tc_flow_log_field_fun = grow_ints(tc_flow_log_field_fun, tc_flow_log_cap, n);
  tc_flow_log_cap = n;
}
void tc_flow_append_current(int count) {
  (void)(ensure_tc_flow_log((tc_flow_log_count + count)));
  int i = 0;
  while (i < count) {
    int slot = (tc_flow_log_count + i);
    tc_flow_log_owned[slot] = tc_var_owned[i];
    tc_flow_log_moved[slot] = tc_var_moved[i];
    tc_flow_log_borrow_count[slot] = tc_var_borrow_count[i];
    tc_flow_log_borrow_mut[slot] = tc_var_borrow_mut[i];
    tc_flow_log_source[slot] = tc_var_borrow_source[i];
    tc_flow_log_mode[slot] = tc_var_borrow_mode[i];
    tc_flow_log_parent[slot] = tc_var_borrow_parent[i];
    tc_flow_log_borrow_param[slot] = tc_var_borrow_param[i];
    tc_flow_log_ffi[slot] = tc_var_ffi_borrowed[i];
    tc_flow_log_closure_moved[slot] = tc_var_closure_moved[i];
    tc_flow_log_fun_target[slot] = tc_var_fun_target[i];
    i = (i + 1);
  }
  tc_flow_log_count = (tc_flow_log_count + count);
}
void tc_flow_append_fields(void) {
  (void)(ensure_tc_flow_log((tc_flow_field_log_count + tc_field_target_count)));
  int i = 0;
  while (i < tc_field_target_count) {
    int slot = (tc_flow_field_log_count + i);
    tc_flow_log_field_root[slot] = tc_field_target_root[i];
    tc_flow_log_field_name[slot] = tc_field_target_name[i];
    tc_flow_log_field_fun[slot] = tc_field_target_fun[i];
    i = (i + 1);
  }
  tc_flow_field_log_count = (tc_flow_field_log_count + tc_field_target_count);
}
int tc_flow_field_log_find(int start, int count, int root, int name) {
  int i = 0;
  while (i < count) {
    int slot = (start + i);
    if ((tc_flow_log_field_root[slot] == root) && (tc_flow_log_field_name[slot] == name))
      return slot;
    else {
    }
    i = (i + 1);
  }
  return (0 - 1);
}
void tc_flow_restore_fields(int frame) {
  int start = tc_flow_frame_field_base[frame];
  int count = tc_flow_frame_field_count[frame];
  int i = 0;
  tc_field_target_count = 0;
  (void)(ensure_tc_field_targets(count));
  while (i < count) {
    int slot = (start + i);
    tc_field_target_root[i] = tc_flow_log_field_root[slot];
    tc_field_target_name[i] = tc_flow_log_field_name[slot];
    tc_field_target_fun[i] = tc_flow_log_field_fun[slot];
    i = (i + 1);
  }
  tc_field_target_count = count;
}
void tc_flow_save_base(void) {
  (void)(ensure_tc_flow_frames(tc_flow_depth));
  tc_flow_frame_base[tc_flow_depth] = tc_flow_log_count;
  tc_flow_frame_count[tc_flow_depth] = tc_var_count;
  tc_flow_frame_yes[tc_flow_depth] = (0 - 1);
  tc_flow_frame_field_base[tc_flow_depth] = tc_flow_field_log_count;
  tc_flow_frame_field_count[tc_flow_depth] = tc_field_target_count;
  tc_flow_frame_field_yes[tc_flow_depth] = (0 - 1);
  tc_flow_frame_field_yes_count[tc_flow_depth] = 0;
  tc_flow_frame_has_yes[tc_flow_depth] = 0;
  (void)(tc_flow_append_current(tc_var_count));
  (void)(tc_flow_append_fields());
  tc_flow_depth = (tc_flow_depth + 1);
}
void tc_flow_save_yes(void) {
  int frame = (tc_flow_depth - 1);
  if (frame < 0)
    return;
  else {
  }
  tc_flow_frame_yes[frame] = tc_flow_log_count;
  tc_flow_frame_field_yes[frame] = tc_flow_field_log_count;
  tc_flow_frame_field_yes_count[frame] = tc_field_target_count;
  (void)(tc_flow_append_current(tc_flow_frame_count[frame]));
  (void)(tc_flow_append_fields());
  tc_var_count = tc_flow_frame_count[frame];
  (void)(tc_flow_restore_fields(frame));
  tc_flow_frame_has_yes[frame] = 1;
}
void tc_flow_restore_base(void) {
  int frame = (tc_flow_depth - 1);
  if (frame < 0)
    return;
  else {
  }
  int start = tc_flow_frame_base[frame];
  int count = tc_flow_frame_count[frame];
  int i = 0;
  while (i < count) {
    int slot = (start + i);
    tc_var_owned[i] = tc_flow_log_owned[slot];
    tc_var_moved[i] = tc_flow_log_moved[slot];
    tc_var_borrow_count[i] = tc_flow_log_borrow_count[slot];
    tc_var_borrow_mut[i] = tc_flow_log_borrow_mut[slot];
    tc_var_borrow_source[i] = tc_flow_log_source[slot];
    tc_var_borrow_mode[i] = tc_flow_log_mode[slot];
    tc_var_borrow_parent[i] = tc_flow_log_parent[slot];
    tc_var_borrow_param[i] = tc_flow_log_borrow_param[slot];
    tc_var_ffi_borrowed[i] = tc_flow_log_ffi[slot];
    tc_var_closure_moved[i] = tc_flow_log_closure_moved[slot];
    tc_var_fun_target[i] = tc_flow_log_fun_target[slot];
    i = (i + 1);
  }
  tc_var_count = count;
  (void)(tc_flow_restore_fields(frame));
}
void tc_flow_merge_yes(void) {
  int frame = (tc_flow_depth - 1);
  if (frame < 0)
    return;
  else {
  }
  if (tc_flow_frame_has_yes[frame] == 0) {
    (void)(tc_flow_save_yes());
    return;
  } else {
  }
  int base = tc_flow_frame_base[frame];
  int yes = tc_flow_frame_yes[frame];
  int count = tc_flow_frame_count[frame];
  int i = 0;
  while (i < count) {
    int slot = (yes + i);
    if ((tc_flow_log_moved[slot] == 1) || (tc_var_moved[i] == 1))
      tc_var_moved[i] = 1;
    else {
    }
    if (((tc_flow_log_owned[slot] == 1) && (tc_var_owned[i] == 1)) && (tc_var_moved[i] == 0))
      tc_var_owned[i] = 1;
    else
      tc_var_owned[i] = 0;
    if (tc_flow_log_borrow_count[slot] > tc_var_borrow_count[i])
      tc_var_borrow_count[i] = tc_flow_log_borrow_count[slot];
    else {
    }
    if (tc_flow_log_borrow_mut[slot] > tc_var_borrow_mut[i])
      tc_var_borrow_mut[i] = tc_flow_log_borrow_mut[slot];
    else {
    }
    if (tc_flow_log_source[slot] == tc_var_borrow_source[i]) {
    } else
      tc_var_borrow_source[i] = (0 - 1);
    if (tc_flow_log_mode[slot] == tc_var_borrow_mode[i]) {
    } else
      tc_var_borrow_mode[i] = 0;
    if (tc_flow_log_parent[slot] == tc_var_borrow_parent[i]) {
    } else
      tc_var_borrow_parent[i] = (0 - 1);
    if (tc_flow_log_borrow_param[slot] == tc_var_borrow_param[i]) {
    } else
      tc_var_borrow_param[i] = 0;
    if ((tc_flow_log_ffi[slot] == 1) || (tc_var_ffi_borrowed[i] == 1))
      tc_var_ffi_borrowed[i] = 1;
    else {
    }
    if ((tc_flow_log_closure_moved[slot] == 1) || (tc_var_closure_moved[i] == 1))
      tc_var_closure_moved[i] = 1;
    else {
    }
    if (tc_flow_log_fun_target[slot] == tc_var_fun_target[i]) {
    } else
      tc_var_fun_target[i] = 0;
    i = (i + 1);
  }
  int field_yes_start = tc_flow_frame_field_yes[frame];
  int field_yes_count = tc_flow_frame_field_yes_count[frame];
  int fi = 0;
  while (fi < tc_field_target_count) {
    int yi = tc_flow_field_log_find(field_yes_start, field_yes_count, tc_field_target_root[fi],
                                    tc_field_target_name[fi]);
    if (yi < 0)
      tc_field_target_fun[fi] = 0;
    else if (tc_flow_log_field_fun[yi] != tc_field_target_fun[fi])
      tc_field_target_fun[fi] = 0;
    else {
    }
    fi = (fi + 1);
  }
  fi = 0;
  while (fi < field_yes_count) {
    int yslot = (field_yes_start + fi);
    if (((tc_flow_log_field_root[yslot] >= 0) && (tc_flow_log_field_root[yslot] < count)) &&
        (tc_field_target_get(tc_flow_log_field_root[yslot], tc_flow_log_field_name[yslot]) == 0))
      (void)(tc_field_target_set(tc_flow_log_field_root[yslot], tc_flow_log_field_name[yslot], 0));
    else {
    }
    fi = (fi + 1);
  }
  tc_var_count = count;
  if (base < 0)
    tc_var_count = 0;
  else {
  }
}
void tc_flow_end(void) {
  int frame = (tc_flow_depth - 1);
  if (frame < 0)
    return;
  else {
  }
  tc_flow_log_count = tc_flow_frame_base[frame];
  tc_flow_field_log_count = tc_flow_frame_field_base[frame];
  tc_flow_depth = frame;
}
void ensure_tc_scopes(int need) {
  if (need < tc_scope_cap)
    return;
  else {
  }
  int n = next_capacity(tc_scope_cap, need);
  tc_scope_start = grow_ints(tc_scope_start, tc_scope_cap, n);
  tc_scope_cap = n;
}
void tc_enter_scope(void) {
  (void)(ensure_tc_scopes(tc_scope_count));
  tc_scope_start[tc_scope_count] = tc_var_count;
  tc_scope_count = (tc_scope_count + 1);
}
void tc_leave_scope(void) {
  int begin = 0;
  int i = 0;
  int source_index = 0;
  if (tc_scope_count == 0)
    return;
  else {
  }
  tc_scope_count = (tc_scope_count - 1);
  begin = tc_scope_start[tc_scope_count];
  i = begin;
  while (i < tc_var_count) {
    source_index = tc_var_borrow_source[i];
    if (source_index < 0) {
    } else if (source_index < begin) {
      if (tc_var_borrow_mode[i] == 3) {
        if (tc_var_borrow_mut[source_index] > 0)
          tc_var_borrow_mut[source_index] = (tc_var_borrow_mut[source_index] - 1);
        else {
        }
      } else if (tc_var_borrow_mode[i] == 2) {
        if (tc_var_borrow_count[source_index] > 0)
          tc_var_borrow_count[source_index] = (tc_var_borrow_count[source_index] - 1);
        else {
        }
      } else {
      }
    } else {
    }
    int closure_cap = tc_var_closure_caps[i];
    while (closure_cap != 0) {
      int closure_source = node_c[closure_cap];
      if (closure_source >= 0) {
        if (closure_source < begin) {
          if (node_aux[closure_cap] == 3) {
            if (tc_var_borrow_mut[closure_source] > 0)
              tc_var_borrow_mut[closure_source] = (tc_var_borrow_mut[closure_source] - 1);
            else {
            }
          } else {
            if (tc_var_borrow_count[closure_source] > 0)
              tc_var_borrow_count[closure_source] = (tc_var_borrow_count[closure_source] - 1);
            else {
            }
          }
        } else {
        }
      } else {
      }
      closure_cap = node_next[closure_cap];
    }
    i = (i + 1);
  }
  int field_read = 0;
  int field_write = 0;
  while (field_read < tc_field_target_count) {
    if (tc_field_target_root[field_read] < begin) {
      tc_field_target_root[field_write] = tc_field_target_root[field_read];
      tc_field_target_name[field_write] = tc_field_target_name[field_read];
      tc_field_target_fun[field_write] = tc_field_target_fun[field_read];
      field_write = (field_write + 1);
    } else {
    }
    field_read = (field_read + 1);
  }
  tc_field_target_count = field_write;
  tc_var_count = begin;
}
void ensure_tc_path(int need) {
  if (need < tc_path_cap)
    return;
  else {
  }
  int n = next_capacity(tc_path_cap, need);
  tc_path_name = grow_ints(tc_path_name, tc_path_cap, n);
  tc_path_cap = n;
}
void tc_fail(int code) {
  if (tc_ok == 1) {
    tc_ok = 0;
    tc_error_code = code;
    if (tc_error_pos < 0)
      tc_error_pos = current_source_pos;
    else {
    }
  } else {
  }
}
void tc_fail_types(int code, int expected_kind, int found_kind) {
  if (tc_ok == 1) {
    tc_error_expected_kind = expected_kind;
    tc_error_found_kind = found_kind;
  } else {
  }
  (void)(tc_fail(code));
}
void ensure_tc_bindings(int need) {
  if (need < tc_bind_cap)
    return;
  else {
  }
  int n = next_capacity(tc_bind_cap, need);
  tc_bind_name = grow_ints(tc_bind_name, tc_bind_cap, n);
  tc_bind_type = grow_ints(tc_bind_type, tc_bind_cap, n);
  tc_bind_cap = n;
}
void tc_bind_clear(void) {
  tc_bind_count = 0;
}
void tc_bind_push(void) {
  (void)(ensure_tc_bindings(tc_bind_count));
  int need = (tc_bind_stack_count + tc_bind_count);
  if (need >= tc_bind_stack_cap) {
    int n = next_capacity(tc_bind_stack_cap, need);
    tc_bind_stack_name = grow_ints(tc_bind_stack_name, tc_bind_stack_cap, n);
    tc_bind_stack_type = grow_ints(tc_bind_stack_type, tc_bind_stack_cap, n);
    tc_bind_stack_cap = n;
  } else {
  }
  if (tc_bind_frame_depth >= tc_bind_frame_cap) {
    int nframe = next_capacity(tc_bind_frame_cap, tc_bind_frame_depth);
    tc_bind_frame_base = grow_ints(tc_bind_frame_base, tc_bind_frame_cap, nframe);
    tc_bind_frame_size = grow_ints(tc_bind_frame_size, tc_bind_frame_cap, nframe);
    tc_bind_frame_cap = nframe;
  } else {
  }
  tc_bind_frame_base[tc_bind_frame_depth] = tc_bind_stack_count;
  tc_bind_frame_size[tc_bind_frame_depth] = tc_bind_count;
  int i = 0;
  while (i < tc_bind_count) {
    tc_bind_stack_name[(tc_bind_stack_count + i)] = tc_bind_name[i];
    tc_bind_stack_type[(tc_bind_stack_count + i)] = tc_bind_type[i];
    i = (i + 1);
  }
  tc_bind_stack_count = (tc_bind_stack_count + tc_bind_count);
  tc_bind_frame_depth = (tc_bind_frame_depth + 1);
}
void tc_bind_pop(void) {
  if (tc_bind_frame_depth < 1) {
    tc_bind_count = 0;
    return;
  } else {
  }
  tc_bind_frame_depth = (tc_bind_frame_depth - 1);
  int base = tc_bind_frame_base[tc_bind_frame_depth];
  int size = tc_bind_frame_size[tc_bind_frame_depth];
  (void)(ensure_tc_bindings(size));
  int i = 0;
  while (i < size) {
    tc_bind_name[i] = tc_bind_stack_name[(base + i)];
    tc_bind_type[i] = tc_bind_stack_type[(base + i)];
    i = (i + 1);
  }
  tc_bind_count = size;
  tc_bind_stack_count = base;
}
int tc_bind_find(int name) {
  int i = 0;
  while (i < tc_bind_count) {
    if (tc_bind_name[i] == name)
      return tc_bind_type[i];
    else {
    }
    i = (i + 1);
  }
  return 0;
}
int tc_bind_add(int name, int ty) {
  int old = tc_bind_find(name);
  if (old != 0) {
    int compatible = tc_type_equal(old, ty);
    if ((node_kind[old] == TY_PARAM) && (node_kind[ty] == TY_PARAM))
      compatible = tc_param_type_same(old, ty);
    else {
    }
    if (compatible == 0) {
      (void)(tc_fail_types(12, node_kind[old], node_kind[ty]));
      return 0;
    } else {
    }
    return 1;
  } else {
  }
  (void)(ensure_tc_bindings(tc_bind_count));
  tc_bind_name[tc_bind_count] = name;
  tc_bind_type[tc_bind_count] = ty;
  tc_bind_count = (tc_bind_count + 1);
  return 1;
}
int tc_bind_explicit_args(int fun_node, int args) {
  int formal = node_aux[fun_node];
  int actual = args;
  while ((formal != 0) && (actual != 0)) {
    (void)(tc_check_type(actual));
    if (tc_ok == 0) {
      (void)(tc_fail(76));
      return 0;
    } else {
    }
    formal = node_next[formal];
    actual = node_next[actual];
  }
  if ((formal != 0) || (actual != 0)) {
    (void)(tc_fail(76));
    return 0;
  } else {
  }
  formal = node_aux[fun_node];
  actual = args;
  while (formal != 0) {
    if (tc_bind_add(node_a[formal], actual) == 0) {
      (void)(tc_fail(76));
      return 0;
    } else {
    }
    formal = node_next[formal];
    actual = node_next[actual];
  }
  return 1;
}
int tc_is_integer_kind(int kind) {
  if (((((kind == TY_INT) || (kind == TY_BOOL)) || (kind == TY_CHAR)) || (kind == TY_LONG)) ||
      (kind == TY_LLONG))
    return 1;
  else {
  }
  if ((((kind == TY_U8) || (kind == TY_U16)) || (kind == TY_U32)) || (kind == TY_U64))
    return 1;
  else {
  }
  if (((((kind == TY_I8) || (kind == TY_I16)) || (kind == TY_I32)) || (kind == TY_I64)) ||
      (kind == TY_USIZE))
    return 1;
  else {
  }
  return 0;
}
int tc_is_numeric_kind(int kind) {
  if (tc_is_integer_kind(kind) == 1)
    return 1;
  else {
  }
  if ((kind == TY_FLOAT) || (kind == TY_DOUBLE))
    return 1;
  else {
  }
  return 0;
}
int tc_is_fixed_integer_kind(int kind) {
  if ((((kind == TY_U8) || (kind == TY_U16)) || (kind == TY_U32)) || (kind == TY_U64))
    return 1;
  else {
  }
  if (((((kind == TY_I8) || (kind == TY_I16)) || (kind == TY_I32)) || (kind == TY_I64)) ||
      (kind == TY_USIZE))
    return 1;
  else {
  }
  return 0;
}
int tc_is_legacy_integer_kind(int kind) {
  if (((((kind == TY_INT) || (kind == TY_BOOL)) || (kind == TY_CHAR)) || (kind == TY_LONG)) ||
      (kind == TY_LLONG))
    return 1;
  else {
  }
  return 0;
}
int tc_decimal_le(int raw, char *limit) {
  if (raw == 0)
    return 1;
  else {
  }
  int n = sym_len[raw];
  int m = 0;
  while (limit[m] != 0) {
    m = (m + 1);
  }
  if (n < m)
    return 1;
  else {
  }
  if (n > m)
    return 0;
  else {
  }
  int i = 0;
  while (i < n) {
    if (source[(sym_start[raw] + i)] < limit[i])
      return 1;
    else {
    }
    if (source[(sym_start[raw] + i)] > limit[i])
      return 0;
    else {
    }
    i = (i + 1);
  }
  return 1;
}
int tc_literal_fits(int id, int target_kind) {
  if (((id == 0) || (node_kind[id] != N_INT)) || (node_aux[id] == 0))
    return 1;
  else {
  }
  if (target_kind == TY_INT)
    return tc_decimal_le(node_aux[id], "2147483647");
  else {
  }
  if (target_kind == TY_U8)
    return tc_decimal_le(node_aux[id], "255");
  else {
  }
  if (target_kind == TY_U16)
    return tc_decimal_le(node_aux[id], "65535");
  else {
  }
  if (target_kind == TY_U32)
    return tc_decimal_le(node_aux[id], "4294967295");
  else {
  }
  if (target_kind == TY_U64)
    return tc_decimal_le(node_aux[id], "18446744073709551615");
  else {
  }
  if (target_kind == TY_I8)
    return tc_decimal_le(node_aux[id], "127");
  else {
  }
  if (target_kind == TY_I16)
    return tc_decimal_le(node_aux[id], "32767");
  else {
  }
  if (target_kind == TY_I32)
    return tc_decimal_le(node_aux[id], "2147483647");
  else {
  }
  if (target_kind == TY_I64)
    return tc_decimal_le(node_aux[id], "9223372036854775807");
  else {
  }
  if (target_kind == TY_USIZE)
    return tc_decimal_le(node_aux[id], "18446744073709551615");
  else {
  }
  return 1;
}
int tc_negative_integer_literal_conversion(int id, int target_kind, int actual_kind) {
  if (((id == 0) || (node_kind[id] != N_BINOP)) || (node_value[id] != OP_SUB))
    return 0;
  else {
  }
  if ((node_kind[node_a[id]] != N_INT) || (node_kind[node_b[id]] != N_INT))
    return 0;
  else {
  }
  if ((node_value[node_a[id]] != 0) || (node_value[node_b[id]] < 0))
    return 0;
  else {
  }
  if ((tc_is_integer_kind(target_kind) == 0) || (tc_is_integer_kind(actual_kind) == 0))
    return 0;
  else {
  }
  if (((((target_kind == TY_U8) || (target_kind == TY_U16)) || (target_kind == TY_U32)) ||
       (target_kind == TY_U64)) ||
      (target_kind == TY_USIZE))
    return 0;
  else {
  }
  if (target_kind == TY_I8) {
    if (node_value[node_b[id]] <= 128)
      return 1;
    else {
    }
    return 0;
  } else {
  }
  if (target_kind == TY_I16) {
    if (node_value[node_b[id]] <= 32768)
      return 1;
    else {
    }
    return 0;
  } else {
  }
  if ((target_kind == TY_I32) || (target_kind == TY_INT)) {
    if (node_value[node_b[id]] <= 2147483647)
      return 1;
    else {
    }
    return 0;
  } else {
  }
  if (((target_kind == TY_I64) || (target_kind == TY_LONG)) || (target_kind == TY_LLONG)) {
    if (node_value[node_b[id]] <= 2147483647)
      return 1;
    else {
    }
    return 0;
  } else {
  }
  return 0;
}
int tc_integer_literal_conversion(int id, int target_kind, int actual_kind) {
  if ((tc_is_integer_kind(target_kind) == 0) || (tc_is_integer_kind(actual_kind) == 0))
    return 0;
  else {
  }
  if ((id != 0) && (node_kind[id] == N_INT)) {
    if (tc_literal_fits(id, target_kind) == 0)
      return 0;
    else {
    }
    return 1;
  } else {
  }
  return tc_negative_integer_literal_conversion(id, target_kind, actual_kind);
}
int tc_type_is_structural(int kind) {
  if (((((((kind == TY_PTR) || (kind == TY_ARRAY)) || (kind == TY_DYN_ARRAY)) ||
         (kind == TY_GENERIC)) ||
        (kind == TY_TUPLE)) ||
       (kind == TY_CLOSURE)) ||
      (kind == TY_VARIANT))
    return 1;
  else {
  }
  return 0;
}
int tc_type_equal(int a, int b) {
  if ((a == 0) || (b == 0))
    return 0;
  else {
  }
  int ak = node_kind[a];
  int bk = node_kind[b];
  if ((ak == TY_PARAM) || (bk == TY_PARAM)) {
    if (ak == bk)
      return 1;
    else {
    }
    return 0;
  } else {
  }
  if (ak != bk)
    return 0;
  else {
  }
  if ((((((((((ak == TY_INT) || (ak == TY_BOOL)) || (ak == TY_STRING)) || (ak == TY_CHAR)) ||
           (ak == TY_FLOAT)) ||
          (ak == TY_DOUBLE)) ||
         (ak == TY_LONG)) ||
        (ak == TY_LLONG)) ||
       (ak == TY_VOID)) ||
      (tc_is_fixed_integer_kind(ak) == 1))
    return 1;
  else {
  }
  if (ak == TY_NAMED) {
    if (node_value[a] == node_value[b])
      return 1;
    else {
    }
    return 0;
  } else {
  }
  if (ak == TY_VARIANT) {
    if ((node_value[a] == node_value[b]) && (node_aux[a] == node_aux[b]))
      return 1;
    else {
    }
    return 0;
  } else {
  }
  if (ak == TY_PTR)
    return tc_type_equal(node_a[a], node_a[b]);
  else {
  }
  if (ak == TY_ARRAY)
    return ((node_value[a] == node_value[b]) && tc_type_equal(node_a[a], node_a[b]));
  else {
  }
  if (ak == TY_DYN_ARRAY)
    return tc_type_equal(node_a[a], node_a[b]);
  else {
  }
  if (ak == TY_GENERIC) {
    if (node_value[a] != node_value[b])
      return 0;
    else {
    }
    int x = node_a[a];
    int y = node_a[b];
    while ((x != 0) && (y != 0)) {
      if (tc_type_equal(x, y) == 0)
        return 0;
      else {
      }
      x = node_next[x];
      y = node_next[y];
    }
    if ((x != 0) || (y != 0))
      return 0;
    else {
    }
    return 1;
  } else {
  }
  if (ak == TY_TUPLE) {
    int x = node_a[a];
    int y = node_a[b];
    while ((x != 0) && (y != 0)) {
      if (tc_type_equal(x, y) == 0)
        return 0;
      else {
      }
      x = node_next[x];
      y = node_next[y];
    }
    if ((x != 0) || (y != 0))
      return 0;
    else {
    }
    return 1;
  } else {
  }
  if ((ak == TY_FUN) || (ak == TY_CLOSURE)) {
    if (tc_type_equal(node_b[a], node_b[b]) == 0)
      return 0;
    else {
    }
    int x = node_a[a];
    int y = node_a[b];
    while ((x != 0) && (y != 0)) {
      if (tc_type_equal(x, y) == 0)
        return 0;
      else {
      }
      x = node_next[x];
      y = node_next[y];
    }
    if ((x != 0) || (y != 0))
      return 0;
    else {
    }
    return 1;
  } else {
  }
  return 1;
}
int tc_signature_type(int entry) {
  if (entry == 0)
    return 0;
  else {
  }
  int args = 0;
  int p = node_c[entry];
  while (p != 0) {
    int src = node_b[p];
    int q = ast_node(node_kind[src], node_a[src], node_b[src], node_c[src], node_value[src],
                     node_aux[src]);
    if (args == 0)
      args = q;
    else
      args = ast_link(args, q);
    p = node_next[p];
  }
  int ret = node_b[entry];
  if (ret != 0)
    ret = ast_node(node_kind[ret], node_a[ret], node_b[ret], node_c[ret], node_value[ret],
                   node_aux[ret]);
  else {
  }
  return ast_node(TY_FUN, args, ret, 0, 0, 0);
}
int tc_type_node_from_summary(int kind, int name, int elem_kind, int elem_name) {
  if (kind == TY_GENERIC) {
    if (name != 0)
      return name;
    else {
    }
  } else {
  }
  if (kind == TY_TUPLE) {
    if (name != 0)
      return name;
    else {
    }
  } else {
  }
  if (kind == TY_PARAM) {
    if ((name != 0) && (node_kind[name] == TY_PARAM))
      return name;
    else {
    }
  } else {
  }
  if (kind == TY_ARRAY) {
    if ((name != 0) && (node_kind[name] == TY_ARRAY))
      return name;
    else {
    }
  } else {
  }
  if (kind == TY_CLOSURE) {
    if (name != 0)
      return name;
    else {
    }
  } else {
  }
  if (kind == TY_NAMED)
    return ast_node(TY_NAMED, 0, 0, 0, name, 0);
  else {
  }
  if (kind == TY_VARIANT)
    return ast_node(TY_VARIANT, 0, 0, 0, name, elem_name);
  else {
  }
  if ((kind == TY_PTR) || (kind == TY_DYN_ARRAY)) {
    int child = 0;
    if ((elem_name != 0) && (tc_type_is_structural(elem_kind) == 1))
      child = elem_name;
    else if (((elem_kind == TY_PARAM) && (elem_name != 0)) && (node_kind[elem_name] == TY_PARAM))
      child = elem_name;
    else
      child = tc_type_node_from_summary(elem_kind, elem_name, 0, 0);
    return ast_node(kind, child, 0, 0, 0, 0);
  } else {
  }
  return ast_node(kind, 0, 0, 0, 0, 0);
}
int tc_generic_moves_array(int fun_node) {
  if (fun_node == 0)
    return 0;
  else {
  }
  int ret = node_b[fun_node];
  if ((ret != 0) && (node_kind[ret] == TY_DYN_ARRAY))
    return 1;
  else {
  }
  int name = node_value[fun_node];
  if ((name == 0) || (sym_len[name] < 4))
    return 0;
  else {
  }
  int s = sym_start[name];
  int n = sym_len[name];
  if ((((source[((s + n) - 4)] == 102) && (source[((s + n) - 3)] == 114)) &&
       (source[((s + n) - 2)] == 101)) &&
      (source[((s + n) - 1)] == 101))
    return 1;
  else {
  }
  return 0;
}
void tc_mark_float_expr(int id, int expected_kind) {
  if (id == 0)
    return;
  else {
  }
  if (node_kind[id] == N_FLOAT) {
    if (expected_kind == TY_FLOAT)
      node_aux[id] = TY_FLOAT;
    else if (expected_kind == TY_DOUBLE)
      node_aux[id] = 0;
    else {
    }
  } else if ((expected_kind == TY_FLOAT) && (node_kind[id] == N_BINOP)) {
    (void)(tc_mark_float_expr(node_a[id], TY_FLOAT));
    (void)(tc_mark_float_expr(node_b[id], TY_FLOAT));
  } else {
  }
}
void tc_match_generic_call_arg(int formal, int actual, int expr) {
  if (((formal != 0) && (expr != 0)) && (node_kind[formal] == TY_PARAM)) {
    int bound = tc_bind_find(node_value[formal]);
    if (((node_kind[expr] == N_INT) && (bound != 0)) && (tc_is_integer_kind(node_kind[bound]) == 1))
      return;
    else {
    }
  } else {
  }
  (void)(tc_match_generic(formal, actual));
}
void tc_match_generic(int formal, int actual) {
  if ((formal == 0) || (actual == 0)) {
    (void)(tc_fail_types(12, 0, 0));
    return;
  } else {
  }
  if (node_kind[formal] == TY_PARAM) {
    if (tc_bind_add(node_value[formal], actual) == 0)
      return;
    else {
    }
    return;
  } else {
  }
  if (node_kind[formal] == TY_GENERIC) {
    (void)(tc_check_type(formal));
    (void)(tc_check_type(actual));
    if ((node_kind[actual] != TY_GENERIC) || (node_value[formal] != node_value[actual])) {
      (void)(tc_fail_types(12, node_kind[formal], node_kind[actual]));
      return;
    } else {
    }
    int f = node_a[formal];
    int a = node_a[actual];
    while ((f != 0) && (a != 0)) {
      (void)(tc_match_generic(f, a));
      f = node_next[f];
      a = node_next[a];
    }
    if ((f != 0) || (a != 0))
      (void)(tc_fail(13));
    else {
    }
    return;
  } else {
  }
  if (node_kind[formal] == TY_TUPLE) {
    if (node_kind[actual] != TY_TUPLE) {
      (void)(tc_fail_types(12, node_kind[formal], node_kind[actual]));
      return;
    } else {
    }
    int f = node_a[formal];
    int a = node_a[actual];
    while ((f != 0) && (a != 0)) {
      (void)(tc_match_generic(f, a));
      f = node_next[f];
      a = node_next[a];
    }
    if ((f != 0) || (a != 0))
      (void)(tc_fail(13));
    else {
    }
    return;
  } else {
  }
  if ((node_kind[formal] == TY_FUN) && (node_kind[actual] == TY_FUN)) {
    int fp = node_a[formal];
    int ap = node_a[actual];
    while ((fp != 0) && (ap != 0)) {
      (void)(tc_match_generic(fp, ap));
      fp = node_next[fp];
      ap = node_next[ap];
    }
    if ((fp != 0) || (ap != 0)) {
      (void)(tc_fail(13));
      return;
    } else {
    }
    (void)(tc_match_generic(node_b[formal], node_b[actual]));
    return;
  } else {
  }
  if ((node_kind[formal] == TY_PTR) && (node_kind[actual] == TY_PTR)) {
    (void)(tc_match_generic(node_a[formal], node_a[actual]));
    return;
  } else {
  }
  if ((node_kind[formal] == TY_ARRAY) && (node_kind[actual] == TY_ARRAY)) {
    if (node_value[formal] != node_value[actual]) {
      (void)(tc_fail_types(12, TY_ARRAY, TY_ARRAY));
      return;
    } else {
    }
    (void)(tc_match_generic(node_a[formal], node_a[actual]));
    return;
  } else {
  }
  if ((node_kind[formal] == TY_DYN_ARRAY) && (node_kind[actual] == TY_DYN_ARRAY)) {
    (void)(tc_match_generic(node_a[formal], node_a[actual]));
    return;
  } else {
  }
  (void)(tc_type_node(formal));
  if (tc_type_equal(formal, actual) == 0)
    (void)(tc_fail_types(12, node_kind[formal], node_kind[actual]));
  else {
  }
}
int tc_substitute_type(int ty) {
  if (ty == 0)
    return 0;
  else {
  }
  if (node_kind[ty] == TY_PARAM) {
    int b = tc_bind_find(node_value[ty]);
    if ((b != 0) && (b != ty)) {
      if ((node_kind[b] == TY_PARAM) && (node_value[b] == node_value[ty]))
        return ast_node(TY_PARAM, node_a[ty], node_b[ty], node_c[ty], node_value[ty], node_aux[ty]);
      else {
      }
      return tc_substitute_type(b);
    } else {
    }
    return ast_node(TY_PARAM, node_a[ty], node_b[ty], node_c[ty], node_value[ty], node_aux[ty]);
  } else {
  }
  if (node_kind[ty] == TY_PTR)
    return ast_node(TY_PTR, tc_substitute_type(node_a[ty]), 0, 0, 0, 0);
  else {
  }
  if (node_kind[ty] == TY_ARRAY)
    return ast_node(TY_ARRAY, tc_substitute_type(node_a[ty]), 0, 0, node_value[ty], 0);
  else {
  }
  if (node_kind[ty] == TY_DYN_ARRAY)
    return ast_node(TY_DYN_ARRAY, tc_substitute_type(node_a[ty]), 0, 0, 0, 0);
  else {
  }
  if (node_kind[ty] == TY_GENERIC) {
    int args = 0;
    int p = node_a[ty];
    while (p != 0) {
      int q = tc_substitute_type(p);
      if (args == 0)
        args = q;
      else
        args = ast_link(args, q);
      p = node_next[p];
    }
    int result = ast_node(TY_GENERIC, args, 0, 0, node_value[ty], 0);
    node_scope[result] = node_scope[ty];
    return result;
  } else {
  }
  if (node_kind[ty] == TY_TUPLE) {
    int items = 0;
    int p = node_a[ty];
    while (p != 0) {
      int q = tc_substitute_type(p);
      if (items == 0)
        items = q;
      else
        items = ast_link(items, q);
      p = node_next[p];
    }
    return ast_node(TY_TUPLE, items, 0, 0, 0, 0);
  } else {
  }
  if ((node_kind[ty] == TY_FUN) || (node_kind[ty] == TY_CLOSURE)) {
    int args = 0;
    int p2 = node_a[ty];
    while (p2 != 0) {
      int q2 = tc_substitute_type(p2);
      if (args == 0)
        args = q2;
      else
        args = ast_link(args, q2);
      p2 = node_next[p2];
    }
    return ast_node(node_kind[ty], args, tc_substitute_type(node_b[ty]), 0, 0, 0);
  } else {
  }
  return ast_node(node_kind[ty], node_a[ty], node_b[ty], node_c[ty], node_value[ty], node_aux[ty]);
}
int tc_same(int a_kind, int a_name, int b_kind, int b_name) {
  if ((a_kind == TY_PTR) && (b_kind == TY_PTR))
    return 1;
  else {
  }
  if (a_kind == b_kind) {
    if (a_kind == TY_NAMED) {
      if (a_name == b_name)
        return 1;
      else {
      }
      return 0;
    } else {
    }
    return 1;
  } else {
  }
  if ((tc_is_integer_kind(a_kind) == 1) && (tc_is_integer_kind(b_kind) == 1))
    return 1;
  else {
  }
  if ((a_kind == TY_VOID) && (b_kind == TY_INT))
    return 1;
  else {
  }
  if ((a_kind == TY_INT) && (b_kind == TY_VOID))
    return 1;
  else {
  }
  return 0;
}
int tc_array_elem_same(int a_kind, int a_name, int b_kind, int b_name) {
  if ((tc_is_legacy_integer_kind(a_kind) == 1) && (tc_is_legacy_integer_kind(b_kind) == 1))
    return 1;
  else {
  }
  if (a_kind != b_kind)
    return 0;
  else {
  }
  if (a_kind == TY_NAMED) {
    if (a_name == b_name)
      return 1;
    else {
    }
    return 0;
  } else {
  }
  return 1;
}
int tc_param_type_same(int a_name, int b_name) {
  if (a_name == b_name)
    return 1;
  else {
  }
  if ((a_name == 0) || (b_name == 0))
    return 0;
  else {
  }
  if ((node_kind[a_name] != TY_PARAM) || (node_kind[b_name] != TY_PARAM))
    return 0;
  else {
  }
  if (node_value[a_name] == node_value[b_name])
    return 1;
  else {
  }
  return 0;
}
int tc_same_full(int a_kind, int a_name, int a_elem_kind, int a_elem_name, int b_kind, int b_name,
                 int b_elem_kind, int b_elem_name) {
  if ((a_kind == TY_PARAM) || (b_kind == TY_PARAM)) {
    if ((a_kind == TY_PARAM) && (b_kind == TY_PARAM))
      return tc_param_type_same(a_name, b_name);
    else {
    }
    return 0;
  } else {
  }
  if ((a_kind == TY_GENERIC) || (b_kind == TY_GENERIC)) {
    if ((a_kind == TY_GENERIC) && (b_kind == TY_GENERIC))
      return tc_type_equal(a_name, b_name);
    else {
    }
    return 0;
  } else {
  }
  if ((a_kind == TY_TUPLE) || (b_kind == TY_TUPLE)) {
    if ((a_kind == TY_TUPLE) && (b_kind == TY_TUPLE))
      return tc_type_equal(a_name, b_name);
    else {
    }
    return 0;
  } else {
  }
  if ((a_kind == TY_CLOSURE) || (b_kind == TY_CLOSURE)) {
    if ((a_kind == TY_CLOSURE) && (b_kind == TY_CLOSURE))
      return tc_type_equal(a_name, b_name);
    else {
    }
    return 0;
  } else {
  }
  if ((a_kind == TY_ARRAY) && (b_kind == TY_ARRAY)) {
    if ((a_name != 0) && (b_name != 0))
      return tc_type_equal(a_name, b_name);
    else {
    }
    return 0;
  } else {
  }
  if ((a_kind == TY_PTR) && (b_kind == TY_PTR)) {
    if ((a_elem_kind == TY_VOID) || (b_elem_kind == TY_VOID))
      return 1;
    else {
    }
    if (a_elem_kind != b_elem_kind)
      return 0;
    else {
    }
    if ((a_elem_kind == TY_NAMED) && (a_elem_name != b_elem_name))
      return 0;
    else {
    }
    if (((tc_type_is_structural(a_elem_kind) == 1) && (a_elem_name != 0)) && (b_elem_name != 0))
      return tc_type_equal(a_elem_name, b_elem_name);
    else {
    }
    return 1;
  } else {
  }
  if (a_kind == b_kind) {
    if (a_kind == TY_NAMED) {
      if (a_name == b_name)
        return 1;
      else {
      }
      return 0;
    } else {
    }
    if (a_kind == TY_DYN_ARRAY) {
      if (a_elem_kind != b_elem_kind)
        return 0;
      else {
      }
      if ((a_elem_kind == TY_NAMED) && (a_elem_name != b_elem_name))
        return 0;
      else {
      }
      if (((tc_type_is_structural(a_elem_kind) == 1) && (a_elem_name != 0)) && (b_elem_name != 0))
        return tc_type_equal(a_elem_name, b_elem_name);
      else {
      }
    } else {
    }
    return 1;
  } else {
  }
  if ((tc_is_legacy_integer_kind(a_kind) == 1) && (tc_is_legacy_integer_kind(b_kind) == 1))
    return 1;
  else {
  }
  if (((a_kind == TY_VOID) && (b_kind == TY_INT)) || ((a_kind == TY_INT) && (b_kind == TY_VOID)))
    return 1;
  else {
  }
  return 0;
}
int tc_ptr_diff_ok(int a_kind, int a_elem_kind, int a_elem_name, int b_kind, int b_elem_kind,
                   int b_elem_name) {
  if ((a_kind != TY_PTR) || (b_kind != TY_PTR))
    return 0;
  else {
  }
  if ((a_elem_kind == TY_VOID) || (b_elem_kind == TY_VOID))
    return 0;
  else {
  }
  if (a_elem_kind != b_elem_kind)
    return 0;
  else {
  }
  if ((a_elem_kind == TY_NAMED) && (a_elem_name != b_elem_name))
    return 0;
  else {
  }
  return 1;
}
int tc_find_struct(int name) {
  int item = node_a[tc_root];
  while (item != 0) {
    if (((node_kind[item] == N_STRUCT) || (node_kind[item] == N_GENERIC_STRUCT)) &&
        (node_value[item] == name))
      return item;
    else {
    }
    item = node_next[item];
  }
  return 0;
}
int tc_find_struct_ctx(int name, int ns) {
  int exact = tc_find_struct(name);
  if (exact != 0)
    return exact;
  else {
  }
  if (ns == 0)
    return 0;
  else {
  }
  int scope = ns;
  while (scope != 0) {
    int candidate = sym_qualified(scope, name);
    int item = tc_find_struct(candidate);
    if (item != 0)
      return item;
    else {
    }
    int split = sym_find_last_scope(scope);
    if (split == 0)
      scope = 0;
    else
      scope = sym_prefix_scope(scope, split);
  }
  return 0;
}
int tc_find_enum(int name) {
  int item = node_a[tc_root];
  while (item != 0) {
    if ((node_kind[item] == N_ENUM) && (node_value[item] == name))
      return item;
    else {
    }
    item = node_next[item];
  }
  return 0;
}
int tc_find_enum_ctx(int name, int ns) {
  int exact = tc_find_enum(name);
  if (exact != 0)
    return exact;
  else {
  }
  if (ns == 0)
    return 0;
  else {
  }
  int scope = ns;
  while (scope != 0) {
    int candidate = sym_qualified(scope, name);
    int item = tc_find_enum(candidate);
    if (item != 0)
      return item;
    else {
    }
    int split = sym_find_last_scope(scope);
    if (split == 0)
      scope = 0;
    else
      scope = sym_prefix_scope(scope, split);
  }
  return 0;
}
int tc_generic_arity(int decl) {
  if ((decl == 0) || (node_kind[decl] != N_GENERIC_STRUCT))
    return 0;
  else {
  }
  int n = 0;
  int p = node_c[decl];
  while (p != 0) {
    n = (n + 1);
    p = node_next[p];
  }
  return n;
}
int tc_generic_arg_count(int ty) {
  if ((ty == 0) || (node_kind[ty] != TY_GENERIC))
    return 0;
  else {
  }
  int n = 0;
  int p = node_a[ty];
  while (p != 0) {
    n = (n + 1);
    p = node_next[p];
  }
  return n;
}
int tc_named_exists_ctx(int name, int ns) {
  if (tc_find_struct_ctx(name, ns) != 0)
    return 1;
  else {
  }
  if (tc_find_enum_ctx(name, ns) != 0)
    return 1;
  else {
  }
  return 0;
}
int tc_named_exists(int name) {
  if (tc_find_struct(name) != 0)
    return 1;
  else {
  }
  if (tc_find_enum(name) != 0)
    return 1;
  else {
  }
  return 0;
}
void tc_check_type(int ty) {
  if (ty == 0) {
    (void)(tc_fail(1));
    return;
  } else {
  }
  int k = node_kind[ty];
  if (k == TY_NAMED) {
    int resolved_struct = tc_find_struct_ctx(node_value[ty], node_scope[ty]);
    int resolved_enum = tc_find_enum_ctx(node_value[ty], node_scope[ty]);
    if (resolved_struct != 0)
      node_value[ty] = node_value[resolved_struct];
    else if (resolved_enum != 0)
      node_value[ty] = node_value[resolved_enum];
    else
      (void)(tc_fail(2));
  } else if (k == TY_GENERIC) {
    int s = tc_find_struct_ctx(node_value[ty], node_scope[ty]);
    if ((s == 0) || (node_kind[s] != N_GENERIC_STRUCT))
      (void)(tc_fail(2));
    else {
      node_value[ty] = node_value[s];
      if (tc_generic_arity(s) != tc_generic_arg_count(ty))
        (void)(tc_fail(37));
      else {
      }
      int a = node_a[ty];
      while (a != 0) {
        (void)(tc_check_type(a));
        a = node_next[a];
      }
    }
  } else if (k == TY_PTR)
    (void)(tc_check_type(node_a[ty]));
  else if (k == TY_ARRAY)
    (void)(tc_check_type(node_a[ty]));
  else if (k == TY_DYN_ARRAY)
    (void)(tc_check_type(node_a[ty]));
  else if (k == TY_TUPLE) {
    int item = node_a[ty];
    while (item != 0) {
      (void)(tc_check_type(item));
      item = node_next[item];
    }
  } else if (k == TY_FUN) {
    int p = node_a[ty];
    while (p != 0) {
      (void)(tc_check_type(p));
      p = node_next[p];
    }
    (void)(tc_check_type(node_b[ty]));
  } else {
  }
}
int tc_cycle_struct(int name) {
  int i = 0;
  while (i < tc_path_count) {
    if (tc_path_name[i] == name)
      return 1;
    else {
    }
    i = (i + 1);
  }
  int s = tc_find_struct(name);
  if (s == 0)
    return 0;
  else {
  }
  (void)(ensure_tc_path(tc_path_count));
  tc_path_name[tc_path_count] = name;
  tc_path_count = (tc_path_count + 1);
  int f = node_a[s];
  int bad = 0;
  while (f != 0) {
    if (tc_cycle_type(node_b[f]) == 1)
      bad = 1;
    else {
    }
    f = node_next[f];
  }
  tc_path_count = (tc_path_count - 1);
  return bad;
}
int tc_cycle_type(int ty) {
  if (ty == 0)
    return 0;
  else {
  }
  if (node_kind[ty] == TY_PTR)
    return 0;
  else {
  }
  if (node_kind[ty] == TY_FUN)
    return 0;
  else {
  }
  if (node_kind[ty] == TY_ARRAY)
    return tc_cycle_type(node_a[ty]);
  else {
  }
  if (node_kind[ty] == TY_DYN_ARRAY)
    return 0;
  else {
  }
  if (node_kind[ty] == TY_NAMED)
    return tc_cycle_struct(node_value[ty]);
  else {
  }
  if (node_kind[ty] == TY_GENERIC)
    return 0;
  else {
  }
  return 0;
}
int tc_release_name(int name) {
  if (bi_has_flag(name, BI_FLAG_CONSUME) == 1)
    return 1;
  else {
  }
  return 0;
}
int tc_owned_initializer(int id) {
  if (id == 0)
    return 0;
  else {
  }
  if (node_kind[id] == N_CLOSURE)
    return 1;
  else {
  }
  if (node_kind[id] == N_MOVE)
    return 1;
  else {
  }
  if (node_kind[id] != N_CALL)
    return 0;
  else {
  }
  if (bi_has_flag(node_value[id], BI_FLAG_OWNED) == 1)
    return 1;
  else {
  }
  return 0;
}
int tc_is_owner_kind(int kind) {
  if (kind == TY_CLOSURE)
    return 1;
  else {
  }
  if (kind == TY_DYN_ARRAY)
    return 1;
  else {
  }
  if (kind == TY_GENERIC)
    return 1;
  else {
  }
  return 0;
}
int tc_is_owner_type(int ty) {
  if (ty == 0)
    return 0;
  else {
  }
  if (tc_is_owner_kind(node_kind[ty]) == 1)
    return 1;
  else {
  }
  return 0;
}
int tc_is_place(int id) {
  if (id == 0)
    return 0;
  else {
  }
  if ((((node_kind[id] == N_VAR) || (node_kind[id] == N_DEREF)) || (node_kind[id] == N_INDEX)) ||
      (node_kind[id] == N_FIELD_ACCESS))
    return 1;
  else {
  }
  return 0;
}
int tc_place_is_const(int id) {
  if (id == 0)
    return 0;
  else {
  }
  if (node_kind[id] == N_VAR) {
    if ((tc_lookup_var(node_value[id]) == 1) && (tc_var_const[tc_last_var_index] == 1))
      return 1;
    else {
    }
    return 0;
  } else {
  }
  if ((node_kind[id] == N_FIELD_ACCESS) || (node_kind[id] == N_INDEX)) {
    int base = node_a[id];
    if (((((node_kind[base] == N_VAR) && (tc_lookup_var(node_value[base]) == 1)) &&
          (tc_var_const[tc_last_var_index] == 1)) &&
         (tc_last_var_type != 0)) &&
        (node_kind[tc_last_var_type] == TY_PTR))
      return 0;
    else {
    }
    return tc_place_is_const(base);
  } else {
  }
  if (node_kind[id] == N_DEREF)
    return 0;
  else {
  }
  return 0;
}
int tc_place_root(int id) {
  if (id == 0)
    return (0 - 1);
  else {
  }
  if (node_kind[id] == N_VAR) {
    if (tc_lookup_var(node_value[id]) == 1) {
      if (tc_var_borrow_source[tc_last_var_index] >= 0)
        return tc_var_borrow_source[tc_last_var_index];
      else {
      }
      return tc_last_var_index;
    } else {
    }
    return (0 - 1);
  } else {
  }
  if (node_kind[id] == N_DEREF) {
    (void)(tc_expr(node_a[id]));
    if (tc_expr_borrow_source >= 0)
      return tc_expr_borrow_source;
    else {
    }
    if ((node_kind[node_a[id]] == N_VAR) && (tc_lookup_var(node_value[node_a[id]]) == 1))
      return tc_last_var_index;
    else {
    }
    return (0 - 1);
  } else {
  }
  if ((node_kind[id] == N_INDEX) || (node_kind[id] == N_FIELD_ACCESS)) {
    (void)(tc_expr(node_a[id]));
    if (tc_expr_borrow_source >= 0)
      return tc_expr_borrow_source;
    else {
    }
    if ((node_kind[node_a[id]] == N_VAR) && (tc_lookup_var(node_value[node_a[id]]) == 1))
      return tc_last_var_index;
    else {
    }
  } else {
  }
  return (0 - 1);
}
int tc_place_borrow_param(int id) {
  if (id == 0)
    return 0;
  else {
  }
  if ((node_kind[id] == N_VAR) && (tc_lookup_var(node_value[id]) == 1))
    return tc_var_borrow_param[tc_last_var_index];
  else {
  }
  (void)(tc_expr(id));
  return tc_expr_borrow_param;
}
int tc_nth_arg(int head, int wanted) {
  if (wanted <= 0)
    return 0;
  else {
  }
  int p = head;
  int index = 1;
  while (p != 0) {
    if (index == wanted)
      return p;
    else {
    }
    index = (index + 1);
    p = node_next[p];
  }
  return 0;
}
int tc_provenance(int id) {
  if (id == 0)
    return (0 - 1);
  else {
  }
  if (node_kind[id] == N_VAR) {
    if (tc_lookup_var(node_value[id]) == 0)
      return (0 - 1);
    else {
    }
    if (tc_var_borrow_source[tc_last_var_index] >= 0)
      return tc_var_borrow_source[tc_last_var_index];
    else {
    }
    if (tc_var_borrow_param[tc_last_var_index] > 0)
      return tc_last_var_index;
    else {
    }
    return (0 - 1);
  } else {
  }
  if (node_kind[id] == N_ADDRESS) {
    int address_source = tc_provenance(node_a[id]);
    if (address_source >= 0)
      return address_source;
    else {
    }
    return tc_place_root(node_a[id]);
  } else {
  }
  if (((node_kind[id] == N_DEREF) || (node_kind[id] == N_INDEX)) ||
      (node_kind[id] == N_FIELD_ACCESS)) {
    int place_source = tc_provenance(node_a[id]);
    if (place_source >= 0)
      return place_source;
    else {
    }
    return tc_place_root(node_a[id]);
  } else {
  }
  if (node_kind[id] == N_CALL) {
    int fun = tc_find_function_ctx(node_value[id], node_scope[id]);
    if ((fun != 0) && (tc_fun_return_param[fun] > 0))
      return tc_provenance(tc_nth_arg(node_a[id], tc_fun_return_param[fun]));
    else {
    }
  } else {
  }
  return (0 - 1);
}
int tc_provenance_origin(int id) {
  if (id == 0)
    return (0 - 1);
  else {
  }
  if (node_kind[id] == N_VAR) {
    if (tc_lookup_var(node_value[id]) == 1)
      return tc_last_var_index;
    else {
    }
    return (0 - 1);
  } else {
  }
  if ((((node_kind[id] == N_ADDRESS) || (node_kind[id] == N_DEREF)) ||
       (node_kind[id] == N_INDEX)) ||
      (node_kind[id] == N_FIELD_ACCESS))
    return tc_provenance_origin(node_a[id]);
  else {
  }
  if (node_kind[id] == N_CALL) {
    int fun = tc_find_function_ctx(node_value[id], node_scope[id]);
    if (fun != 0) {
      (void)(ensure_tc_fun_meta(fun));
      if (tc_fun_return_param[fun] > 0)
        return tc_provenance_origin(tc_nth_arg(node_a[id], tc_fun_return_param[fun]));
      else {
      }
    } else {
    }
  } else {
  }
  return (0 - 1);
}
int tc_is_loan_ancestor(int ancestor, int origin) {
  if ((ancestor < 0) || (origin < 0))
    return 0;
  else {
  }
  int current = origin;
  int steps = 0;
  while (((current >= 0) && (current < tc_var_count)) && (steps <= tc_var_count)) {
    if (current == ancestor)
      return 1;
    else {
    }
    current = tc_var_borrow_parent[current];
    steps = (steps + 1);
  }
  return 0;
}
int tc_borrow_conflict_from(int root, int origin, int requested_mut) {
  if (root < 0)
    return 0;
  else {
  }
  int i = 0;
  while (i < tc_var_count) {
    if ((tc_var_borrow_source[i] == root) &&
        ((tc_var_borrow_mode[i] == 2) || (tc_var_borrow_mode[i] == 3))) {
      if ((i == origin) || (tc_is_loan_ancestor(i, origin) == 1)) {
      } else if ((requested_mut == 1) || (tc_var_borrow_mode[i] == 3))
        return 1;
      else {
      }
    } else {
    }
    i = (i + 1);
  }
  return 0;
}
int tc_provenance_param(int id) {
  if (id == 0)
    return 0;
  else {
  }
  if (node_kind[id] == N_VAR) {
    if (tc_lookup_var(node_value[id]) == 1)
      return tc_var_borrow_param[tc_last_var_index];
    else {
    }
    return 0;
  } else {
  }
  if ((((node_kind[id] == N_ADDRESS) || (node_kind[id] == N_DEREF)) ||
       (node_kind[id] == N_INDEX)) ||
      (node_kind[id] == N_FIELD_ACCESS))
    return tc_provenance_param(node_a[id]);
  else {
  }
  if (node_kind[id] == N_CALL) {
    int fun = tc_find_function_ctx(node_value[id], node_scope[id]);
    if ((fun != 0) && (tc_fun_return_param[fun] > 0))
      return tc_provenance_param(tc_nth_arg(node_a[id], tc_fun_return_param[fun]));
    else {
    }
  } else {
  }
  return 0;
}
int tc_provenance_mut(int id) {
  if (id == 0)
    return 0;
  else {
  }
  if (node_kind[id] == N_ADDRESS) {
    if (node_value[id] == 1)
      return 1;
    else {
    }
    return tc_provenance_mut(node_a[id]);
  } else {
  }
  if (node_kind[id] == N_VAR) {
    if ((tc_lookup_var(node_value[id]) == 1) && (tc_var_borrow_mode[tc_last_var_index] == 3))
      return 1;
    else {
    }
    return 0;
  } else {
  }
  if (((node_kind[id] == N_DEREF) || (node_kind[id] == N_INDEX)) ||
      (node_kind[id] == N_FIELD_ACCESS))
    return tc_provenance_mut(node_a[id]);
  else {
  }
  return 0;
}
void ensure_tc_field_targets(int need) {
  if (need < tc_field_target_cap)
    return;
  else {
  }
  int n = next_capacity(tc_field_target_cap, need);
  tc_field_target_root = grow_ints(tc_field_target_root, tc_field_target_cap, n);
  tc_field_target_name = grow_ints(tc_field_target_name, tc_field_target_cap, n);
  tc_field_target_fun = grow_ints(tc_field_target_fun, tc_field_target_cap, n);
  tc_field_target_cap = n;
}
int tc_field_target_get(int root, int name) {
  int i = (tc_field_target_count - 1);
  while (i >= 0) {
    if ((tc_field_target_root[i] == root) && (tc_field_target_name[i] == name))
      return tc_field_target_fun[i];
    else {
    }
    i = (i - 1);
  }
  return 0;
}
void tc_field_target_set(int root, int name, int target) {
  if ((root < 0) || (name == 0))
    return;
  else {
  }
  int i = (tc_field_target_count - 1);
  while (i >= 0) {
    if ((tc_field_target_root[i] == root) && (tc_field_target_name[i] == name)) {
      tc_field_target_fun[i] = target;
      return;
    } else {
    }
    i = (i - 1);
  }
  (void)(ensure_tc_field_targets(tc_field_target_count));
  tc_field_target_root[tc_field_target_count] = root;
  tc_field_target_name[tc_field_target_count] = name;
  tc_field_target_fun[tc_field_target_count] = target;
  tc_field_target_count = (tc_field_target_count + 1);
}
int tc_field_target_root_for_place(int id) {
  if (id == 0)
    return (0 - 1);
  else {
  }
  if (node_kind[id] == N_VAR) {
    if (tc_lookup_var(node_value[id]) == 1)
      return tc_last_var_index;
    else {
    }
    return (0 - 1);
  } else {
  }
  if (((node_kind[id] == N_DEREF) || (node_kind[id] == N_INDEX)) ||
      (node_kind[id] == N_FIELD_ACCESS))
    return tc_place_root(id);
  else {
  }
  return (0 - 1);
}
int tc_fun_type_has_sensitive_param(int ty) {
  if ((ty == 0) || (node_kind[ty] != TY_FUN))
    return 0;
  else {
  }
  int p = node_a[ty];
  while (p != 0) {
    int pk = node_kind[p];
    if (((((pk == TY_PTR) || (pk == TY_STRING)) || (pk == TY_DYN_ARRAY)) || (pk == TY_GENERIC)) ||
        (pk == TY_CLOSURE))
      return 1;
    else {
    }
    p = node_next[p];
  }
  return 0;
}
int tc_contract_param_position(int fun, int name) {
  if (fun == 0)
    return 0;
  else {
  }
  int p = node_c[fun];
  int index = 1;
  while (p != 0) {
    if ((node_a[p] == name) && ((node_aux[p] == 2) || (node_aux[p] == 3)))
      return index;
    else {
    }
    index = (index + 1);
    p = node_next[p];
  }
  return 0;
}
void tc_contract_record(int fun, int candidate, int mutable) {
  if (candidate <= 0)
    return;
  else {
  }
  (void)(ensure_tc_fun_meta(fun));
  if (tc_fun_return_param[fun] == 0) {
    tc_fun_return_param[fun] = candidate;
    tc_fun_return_mut[fun] = mutable;
  } else if ((tc_fun_return_param[fun] != candidate) || (tc_fun_return_mut[fun] != mutable)) {
    tc_fun_return_param[fun] = (0 - 1);
    tc_fun_return_mut[fun] = 0;
  } else {
  }
}
int tc_contract_expr_param(int fun, int id) {
  if (id == 0)
    return 0;
  else {
  }
  if (node_kind[id] == N_VAR)
    return tc_contract_param_position(fun, node_value[id]);
  else {
  }
  if (((((node_kind[id] == N_ADDRESS) || (node_kind[id] == N_DEREF)) ||
        (node_kind[id] == N_INDEX)) ||
       (node_kind[id] == N_FIELD_ACCESS)) ||
      (node_kind[id] == N_MOVE))
    return tc_contract_expr_param(fun, node_a[id]);
  else {
  }
  if (node_kind[id] == N_CALL) {
    int called = tc_find_function_ctx(node_value[id], node_scope[id]);
    if (called != 0) {
      (void)(ensure_tc_fun_meta(called));
      if (tc_fun_return_param[called] > 0)
        return tc_contract_expr_param(fun, tc_nth_arg(node_a[id], tc_fun_return_param[called]));
      else {
      }
    } else {
    }
  } else {
  }
  return 0;
}
int tc_contract_expr_mut(int fun, int id) {
  if (id == 0)
    return 0;
  else {
  }
  if (node_kind[id] == N_ADDRESS)
    return node_value[id];
  else {
  }
  if (node_kind[id] == N_VAR) {
    int p = node_c[fun];
    while (p != 0) {
      if ((node_a[p] == node_value[id]) && (node_aux[p] == 3))
        return 1;
      else {
      }
      p = node_next[p];
    }
    return 0;
  } else {
  }
  if ((((node_kind[id] == N_DEREF) || (node_kind[id] == N_INDEX)) ||
       (node_kind[id] == N_FIELD_ACCESS)) ||
      (node_kind[id] == N_MOVE))
    return tc_contract_expr_mut(fun, node_a[id]);
  else {
  }
  if (node_kind[id] == N_CALL) {
    int called = tc_find_function_ctx(node_value[id], node_scope[id]);
    if (called != 0) {
      (void)(ensure_tc_fun_meta(called));
      if (tc_fun_return_param[called] > 0)
        return tc_fun_return_mut[called];
      else {
      }
    } else {
    }
  } else {
  }
  return 0;
}
void tc_contract_scan_expr(int fun, int id) {
  if (id == 0)
    return;
  else {
  }
  if (((((((node_kind[id] == N_ADDRESS) || (node_kind[id] == N_VAR)) ||
          (node_kind[id] == N_DEREF)) ||
         (node_kind[id] == N_INDEX)) ||
        (node_kind[id] == N_FIELD_ACCESS)) ||
       (node_kind[id] == N_MOVE)) ||
      (node_kind[id] == N_CALL)) {
    int pos = tc_contract_expr_param(fun, id);
    if (pos > 0)
      (void)(tc_contract_record(fun, pos, tc_contract_expr_mut(fun, id)));
    else {
    }
    return;
  } else {
  }
}
void tc_contract_scan_stmt(int fun, int id) {
  if (id == 0)
    return;
  else {
  }
  int k = node_kind[id];
  if (k == N_RETURN)
    (void)(tc_contract_scan_expr(fun, node_a[id]));
  else if (k == N_BLOCK) {
    int p = node_a[id];
    while (p != 0) {
      (void)(tc_contract_scan_stmt(fun, p));
      p = node_next[p];
    }
  } else if (k == N_IF) {
    (void)(tc_contract_scan_stmt(fun, node_b[id]));
    (void)(tc_contract_scan_stmt(fun, node_c[id]));
  } else if (k == N_WHILE)
    (void)(tc_contract_scan_stmt(fun, node_b[id]));
  else if (k == N_FOR) {
    (void)(tc_contract_scan_stmt(fun, node_a[id]));
    (void)(tc_contract_scan_stmt(fun, node_c[id]));
    (void)(tc_contract_scan_stmt(fun, node_value[id]));
  } else if (k == N_MATCH) {
    int arm = node_b[id];
    while (arm != 0) {
      (void)(tc_contract_scan_stmt(fun, node_b[arm]));
      arm = node_next[arm];
    }
  } else {
  }
}
void tc_prepare_return_contracts(int root) {
  int item = node_a[root];
  while (item != 0) {
    if ((node_kind[item] == N_FUNC) || (node_kind[item] == N_GENERIC_FUNC)) {
      (void)(ensure_tc_fun_meta(item));
      if ((node_b[item] != 0) && (node_kind[node_b[item]] == TY_PTR)) {
        tc_fun_return_param[item] = 0;
        tc_fun_return_mut[item] = 0;
        (void)(tc_contract_scan_stmt(item, node_a[item]));
        if (tc_fun_return_param[item] < 0) {
          tc_error_pos = node_pos[item];
          (void)(tc_fail(69));
        } else {
        }
      } else {
      }
    } else {
    }
    item = node_next[item];
  }
}
void tc_check_mutable_place(int id) {
  if (id == 0) {
    (void)(tc_fail(70));
    return;
  } else {
  }
  if (node_kind[id] == N_VAR) {
    if (tc_lookup_var(node_value[id]) == 0) {
      (void)(tc_fail(5));
      return;
    } else {
    }
    if (tc_var_const[tc_last_var_index] == 1) {
      (void)(tc_fail(31));
      return;
    } else {
    }
    if (tc_var_mode[tc_last_var_index] == 2) {
      (void)(tc_fail(70));
      return;
    } else {
    }
    if (tc_borrow_conflict(tc_last_var_index) == 1) {
      (void)(tc_fail(37));
      return;
    } else {
    }
    return;
  } else {
  }
  if (tc_is_place(id) == 0) {
    (void)(tc_fail(70));
    return;
  } else {
  }
  if (tc_place_is_const(id) == 1) {
    (void)(tc_fail(31));
    return;
  } else {
  }
  (void)(tc_expr(id));
  int root = tc_expr_borrow_source;
  if (root < 0)
    root = tc_place_root(id);
  else {
  }
  if (root < 0) {
    (void)(tc_fail(70));
    return;
  } else {
  }
  if (tc_expr_borrow_mut == 1) {
    if (((root < tc_var_count) && (tc_var_borrow_mut[root] == 1)) &&
        (tc_var_borrow_count[root] == 0))
      return;
    else {
    }
  } else {
  }
  (void)(tc_fail(37));
}
int tc_borrow_conflict(int index) {
  if (index < 0)
    return 0;
  else {
  }
  if ((index < tc_var_count) &&
      ((tc_var_borrow_count[index] > 0) || (tc_var_borrow_mut[index] > 0)))
    return 1;
  else {
  }
  return 0;
}
int tc_mut_borrow_conflict(int index) {
  if (index < 0)
    return 0;
  else {
  }
  if ((index < tc_var_count) &&
      ((tc_var_borrow_count[index] > 0) || (tc_var_borrow_mut[index] > 0)))
    return 1;
  else {
  }
  return 0;
}
void tc_move_var(int index) {
  if (index < 0) {
    (void)(tc_fail(5));
    return;
  } else {
  }
  if (index < tc_var_count) {
  } else {
    (void)(tc_fail(5));
    return;
  }
  if (tc_borrow_conflict(index) == 1) {
    (void)(tc_fail(37));
    return;
  } else {
  }
  if (tc_var_moved[index] == 1) {
    (void)(tc_fail(34));
    return;
  } else {
  }
  if (tc_var_owned[index] == 0) {
    (void)(tc_fail(35));
    return;
  } else {
  }
  tc_var_moved[index] = 1;
  tc_var_owned[index] = 0;
}
void tc_move_value(int id) {
  if ((id == 0) || (node_kind[id] != N_VAR))
    return;
  else {
  }
  if (tc_loop_depth > 0) {
    (void)(tc_fail(75));
    return;
  } else {
  }
  if (tc_lookup_var(node_value[id]) == 0) {
    (void)(tc_fail(5));
    return;
  } else {
  }
  if (tc_is_owner_type(tc_last_var_type) == 1)
    (void)(tc_move_var(tc_last_var_index));
  else
    (void)(tc_fail(58));
}
void tc_check_call_borrow(int arg, int mode) {
  if (mode == 0)
    return;
  else {
  }
  if (mode == 1) {
    if ((arg != 0) && (node_kind[arg] == N_MOVE))
      return;
    else {
    }
    (void)(tc_fail(40));
    return;
  } else {
  }
  int source_index = (0 - 1);
  if ((arg != 0) && (node_kind[arg] == N_VAR)) {
    if (tc_lookup_var(node_value[arg]) == 1)
      source_index = tc_last_var_index;
    else {
    }
  } else {
    source_index = tc_expr_borrow_source;
  }
  if (source_index < 0) {
    (void)(tc_fail(59));
    return;
  } else {
  }
  if (source_index >= tc_var_count) {
    (void)(tc_fail(59));
    return;
  } else {
  }
  if (tc_var_moved[source_index] == 1) {
    (void)(tc_fail(33));
    return;
  } else {
  }
  if (mode == 2) {
    if ((tc_var_borrow_mut[source_index] > 0) && (tc_expr_borrow_mut == 0))
      (void)(tc_fail(37));
    else {
    }
  } else if (mode == 3) {
    if ((tc_expr_borrow_mut == 0) &&
        ((tc_var_mode[source_index] == 2) || (tc_var_borrow_mode[source_index] == 2))) {
      (void)(tc_fail(37));
      return;
    } else {
    }
    if (((tc_expr_borrow_mut == 1) && (tc_var_borrow_mut[source_index] == 1)) &&
        (tc_var_borrow_count[source_index] == 0)) {
    } else if ((tc_var_borrow_count[source_index] > 0) || (tc_var_borrow_mut[source_index] > 0))
      (void)(tc_fail(37));
    else {
    }
  } else {
  }
}
void tc_check_return_escape(int source_index) {
  if (source_index < 0)
    return;
  else {
  }
  if (source_index < tc_global_count)
    return;
  else {
  }
  if (((source_index < tc_var_count) && (tc_var_param[source_index] == 1)) &&
      ((tc_var_mode[source_index] == 2) || (tc_var_mode[source_index] == 3)))
    return;
  else {
  }
  (void)(tc_fail(72));
}
void tc_check_explicit_return_address_escape(int source_index) {
  if (source_index < 0)
    return;
  else {
  }
  if (source_index < tc_global_count)
    return;
  else {
  }
  if (((source_index < tc_var_count) && (tc_var_param[source_index] == 1)) &&
      ((tc_var_mode[source_index] == 2) || (tc_var_mode[source_index] == 3)))
    return;
  else {
  }
  (void)(tc_fail(38));
}
void tc_record_borrow_ex(int destination, int source_index2, int mode, int origin) {
  if ((destination < 0) || (source_index2 < 0))
    return;
  else {
  }
  if ((destination >= tc_var_count) || (source_index2 >= tc_var_count))
    return;
  else {
  }
  if (tc_var_moved[source_index2] == 1) {
    (void)(tc_fail(33));
    return;
  } else {
  }
  if (origin < 0)
    origin = source_index2;
  else {
  }
  if (tc_borrow_conflict_from(source_index2, origin, (mode == 3)) == 1) {
    (void)(tc_fail(37));
    return;
  } else {
  }
  if (mode == 3)
    tc_var_borrow_mut[source_index2] = (tc_var_borrow_mut[source_index2] + 1);
  else
    tc_var_borrow_count[source_index2] = (tc_var_borrow_count[source_index2] + 1);
  tc_var_borrow_source[destination] = source_index2;
  tc_var_borrow_mode[destination] = mode;
  tc_var_borrow_parent[destination] = origin;
  if ((tc_var_param[source_index2] == 1) &&
      ((tc_var_mode[source_index2] == 2) || (tc_var_mode[source_index2] == 3)))
    tc_var_borrow_param[destination] = tc_var_param_pos[source_index2];
  else
    tc_var_borrow_param[destination] = tc_var_borrow_param[source_index2];
}
void tc_record_borrow(int destination, int source_index2) {
  (void)(tc_record_borrow_ex(destination, source_index2, 2, source_index2));
}
void tc_record_borrow_mut(int destination, int source_index2) {
  (void)(tc_record_borrow_ex(destination, source_index2, 3, source_index2));
}
void tc_release_borrow(int index) {
  if ((index < 0) || (index >= tc_var_count))
    return;
  else {
  }
  int source_index = tc_var_borrow_source[index];
  if ((source_index >= 0) && (source_index < tc_var_count)) {
    if (tc_var_borrow_mode[index] == 3) {
      if (tc_var_borrow_mut[source_index] > 0)
        tc_var_borrow_mut[source_index] = (tc_var_borrow_mut[source_index] - 1);
      else {
      }
    } else if (tc_var_borrow_mode[index] == 2) {
      if (tc_var_borrow_count[source_index] > 0)
        tc_var_borrow_count[source_index] = (tc_var_borrow_count[source_index] - 1);
      else {
      }
    } else {
    }
  } else {
  }
  tc_var_borrow_source[index] = (0 - 1);
  tc_var_borrow_mode[index] = 0;
  tc_var_borrow_parent[index] = (0 - 1);
  tc_var_borrow_param[index] = 0;
}
void tc_require_mutable(int id) {
  if (id == 0)
    return;
  else {
  }
  if (node_kind[id] == N_VAR) {
    if (tc_lookup_var(node_value[id]) == 1) {
      if (tc_var_const[tc_last_var_index] == 1) {
        (void)(tc_fail(31));
        return;
      } else {
      }
      if (tc_var_mode[tc_last_var_index] == 2)
        (void)(tc_fail(37));
      else if (tc_borrow_conflict(tc_last_var_index) == 1)
        (void)(tc_fail(37));
      else {
      }
    } else {
    }
    return;
  } else {
  }
  if (tc_is_place(id) == 0) {
    (void)(tc_fail(70));
    return;
  } else {
  }
  if (tc_place_is_const(id) == 1) {
    (void)(tc_fail(31));
    return;
  } else {
  }
  (void)(tc_expr(id));
  if (tc_expr_borrow_source < 0)
    return;
  else {
  }
  if (tc_expr_borrow_mut == 1) {
    if (tc_borrow_conflict_from(tc_expr_borrow_source, tc_expr_borrow_origin, 1) == 0)
      return;
    else {
    }
  } else {
  }
  (void)(tc_fail(37));
}
void tc_consume_call(int id) {
  if (((id == 0) || (node_kind[id] != N_CALL)) || (tc_release_name(node_value[id]) == 0))
    return;
  else {
  }
  int arg = node_a[id];
  if ((arg == 0) || (node_kind[arg] != N_VAR))
    return;
  else {
  }
  if (tc_lookup_var(node_value[arg]) == 0) {
    (void)(tc_fail(5));
    return;
  } else {
  }
  if (tc_last_var_moved == 1) {
    (void)(tc_fail(34));
    return;
  } else {
  }
  if (tc_last_var_owned == 0) {
    (void)(tc_fail(35));
    return;
  } else {
  }
  if (tc_borrow_conflict(tc_last_var_index) == 1) {
    (void)(tc_fail(37));
    return;
  } else {
  }
  tc_var_moved[tc_last_var_index] = 1;
  tc_var_owned[tc_last_var_index] = 0;
}
void tc_add_var(int name, int kind, int named, int elem_kind, int elem_name, int type_node) {
  int begin = 0;
  if (tc_scope_count > 0)
    begin = tc_scope_start[(tc_scope_count - 1)];
  else {
  }
  int i = 0;
  while (i < tc_var_count) {
    if ((tc_var_name[i] == name) && ((tc_allow_outer_shadow == 0) || (i >= begin))) {
      (void)(tc_fail(3));
      return;
    } else {
    }
    i = (i + 1);
  }
  (void)(ensure_tc_vars(tc_var_count));
  tc_var_name[tc_var_count] = name;
  tc_var_kind[tc_var_count] = kind;
  tc_var_named[tc_var_count] = named;
  tc_var_elem_kind[tc_var_count] = elem_kind;
  tc_var_elem_name[tc_var_count] = elem_name;
  tc_var_type[tc_var_count] = type_node;
  tc_var_fun_target[tc_var_count] = 0;
  tc_var_owned[tc_var_count] = 0;
  tc_var_moved[tc_var_count] = 0;
  tc_var_borrow_count[tc_var_count] = 0;
  tc_var_borrow_mut[tc_var_count] = 0;
  tc_var_borrow_source[tc_var_count] = (0 - 1);
  tc_var_borrow_mode[tc_var_count] = 0;
  tc_var_borrow_parent[tc_var_count] = (0 - 1);
  tc_var_borrow_param[tc_var_count] = 0;
  tc_var_param[tc_var_count] = 0;
  tc_var_param_pos[tc_var_count] = 0;
  tc_var_mode[tc_var_count] = 0;
  tc_var_const[tc_var_count] = 0;
  tc_var_closure_caps[tc_var_count] = 0;
  tc_var_closure_moved[tc_var_count] = 0;
  tc_var_ffi_borrowed[tc_var_count] = 0;
  tc_last_var_index = tc_var_count;
  sym_type[name] = kind;
  sym_elem_kind[name] = elem_kind;
  sym_elem_name[name] = elem_name;
  tc_var_count = (tc_var_count + 1);
}
int tc_lookup_var(int name) {
  tc_last_var_type = 0;
  tc_last_var_fun_target = 0;
  tc_last_var_owned = 0;
  tc_last_var_moved = 0;
  tc_last_var_ffi_borrowed = 0;
  tc_expr_borrow_source = (0 - 1);
  tc_expr_borrow_origin = (0 - 1);
  tc_expr_borrow_mut = 0;
  tc_expr_borrow_param = 0;
  tc_expr_owner_source = (0 - 1);
  tc_expr_is_owned = 0;
  tc_last_var_index = 0;
  int i = (tc_var_count - 1);
  while (1 == 1) {
    if (i < 0)
      return 0;
    else {
    }
    if (tc_var_name[i] == name) {
      tc_kind = tc_var_kind[i];
      tc_name = tc_var_named[i];
      tc_elem_kind = tc_var_elem_kind[i];
      tc_elem_name = tc_var_elem_name[i];
      tc_last_var_type = tc_var_type[i];
      tc_last_var_fun_target = tc_var_fun_target[i];
      if (tc_kind == TY_ARRAY)
        tc_name = tc_last_var_type;
      else {
      }
      if ((tc_kind == TY_DYN_ARRAY) && (tc_last_var_type != 0)) {
        tc_elem_kind = node_kind[node_a[tc_last_var_type]];
        if (tc_elem_kind == TY_NAMED)
          tc_elem_name = node_value[node_a[tc_last_var_type]];
        else {
        }
      } else {
      }
      tc_last_var_owned = tc_var_owned[i];
      tc_last_var_moved = tc_var_moved[i];
      tc_last_var_ffi_borrowed = tc_var_ffi_borrowed[i];
      tc_expr_borrow_source = tc_var_borrow_source[i];
      if ((tc_expr_borrow_source < 0) && (tc_var_borrow_param[i] > 0))
        tc_expr_borrow_source = i;
      else {
      }
      tc_expr_borrow_origin = i;
      tc_expr_borrow_mut = 0;
      if (tc_var_borrow_mode[i] == 3)
        tc_expr_borrow_mut = 1;
      else {
      }
      tc_expr_borrow_param = tc_var_borrow_param[i];
      tc_last_var_index = i;
      return 1;
    } else {
    }
    i = (i - 1);
  }
  return 0;
}
void tc_type_node(int ty) {
  (void)(tc_check_type(ty));
  tc_result_type = ty;
  tc_elem_kind = 0;
  tc_elem_name = 0;
  if (ty == 0) {
    tc_kind = TY_VOID;
    tc_name = 0;
    return;
  } else {
  }
  tc_kind = node_kind[ty];
  if (tc_kind == TY_NAMED) {
    tc_name = node_value[ty];
  } else if (tc_kind == TY_ARRAY) {
    tc_name = ty;
  } else if ((((tc_kind == TY_GENERIC) || (tc_kind == TY_TUPLE)) || (tc_kind == TY_CLOSURE)) ||
             (tc_kind == TY_PARAM)) {
    tc_name = ty;
  } else {
    tc_name = 0;
  }
  if (((tc_kind == TY_PTR) || (tc_kind == TY_DYN_ARRAY)) && (node_a[ty] != 0)) {
    tc_elem_kind = node_kind[node_a[ty]];
    if (tc_elem_kind == TY_NAMED) {
      tc_elem_name = node_value[node_a[ty]];
    } else if (tc_type_is_structural(tc_elem_kind) == 1) {
      tc_elem_name = node_a[ty];
    } else if (tc_elem_kind == TY_PARAM) {
      tc_elem_name = node_a[ty];
    } else {
    }
  } else {
  }
}
int tc_numeric_result_kind(int a, int b) {
  if ((a == TY_DOUBLE) || (b == TY_DOUBLE))
    return TY_DOUBLE;
  else {
  }
  if ((a == TY_FLOAT) || (b == TY_FLOAT))
    return TY_FLOAT;
  else {
  }
  if ((a == b) && (tc_is_fixed_integer_kind(a) == 1))
    return a;
  else {
  }
  if ((a == TY_USIZE) || (b == TY_USIZE))
    return TY_USIZE;
  else {
  }
  if ((a == TY_U64) || (b == TY_U64))
    return TY_U64;
  else {
  }
  if ((a == TY_I64) || (b == TY_I64))
    return TY_I64;
  else {
  }
  if ((a == TY_U32) || (b == TY_U32))
    return TY_U32;
  else {
  }
  if ((a == TY_I32) || (b == TY_I32))
    return TY_I32;
  else {
  }
  if ((a == TY_U16) || (b == TY_U16))
    return TY_U16;
  else {
  }
  if ((a == TY_I16) || (b == TY_I16))
    return TY_I16;
  else {
  }
  if ((a == TY_U8) || (b == TY_U8))
    return TY_U8;
  else {
  }
  if ((a == TY_I8) || (b == TY_I8))
    return TY_I8;
  else {
  }
  if ((a == TY_LLONG) || (b == TY_LLONG))
    return TY_LLONG;
  else {
  }
  if ((a == TY_LONG) || (b == TY_LONG))
    return TY_LONG;
  else {
  }
  return TY_INT;
}
int tc_integer_result_kind(int a, int b) {
  if ((a == b) && (tc_is_fixed_integer_kind(a) == 1))
    return a;
  else {
  }
  if ((a == TY_USIZE) || (b == TY_USIZE))
    return TY_USIZE;
  else {
  }
  if ((a == TY_U64) || (b == TY_U64))
    return TY_U64;
  else {
  }
  if ((a == TY_I64) || (b == TY_I64))
    return TY_I64;
  else {
  }
  if ((a == TY_U32) || (b == TY_U32))
    return TY_U32;
  else {
  }
  if ((a == TY_I32) || (b == TY_I32))
    return TY_I32;
  else {
  }
  if ((a == TY_U16) || (b == TY_U16))
    return TY_U16;
  else {
  }
  if ((a == TY_I16) || (b == TY_I16))
    return TY_I16;
  else {
  }
  if ((a == TY_U8) || (b == TY_U8))
    return TY_U8;
  else {
  }
  if ((a == TY_I8) || (b == TY_I8))
    return TY_I8;
  else {
  }
  if ((a == TY_LLONG) || (b == TY_LLONG))
    return TY_LLONG;
  else {
  }
  if ((a == TY_LONG) || (b == TY_LONG))
    return TY_LONG;
  else {
  }
  return TY_INT;
}
int tc_check_variant(int id) {
  if (tc_find_enum_variant(node_value[id]) == 0)
    return 0;
  else {
  }
  int variant_enum = tc_variant_enum;
  int arg = node_a[id];
  int field = node_b[tc_variant_member];
  while ((arg != 0) && (field != 0)) {
    (void)(tc_type_node(node_b[field]));
    int fk = tc_kind;
    int f_name = tc_name;
    int fek = tc_elem_kind;
    int f_elem_name = tc_elem_name;
    (void)(tc_mark_float_expr(arg, fk));
    (void)(tc_expr(arg));
    int ak = tc_kind;
    int an = tc_name;
    int aek = tc_elem_kind;
    int aen = tc_elem_name;
    if (tc_literal_fits(arg, fk) == 0)
      (void)(tc_fail(54));
    else {
    }
    if (tc_same_full(ak, an, aek, aen, fk, f_name, fek, f_elem_name) == 0) {
      if (tc_integer_literal_conversion(arg, fk, ak) == 0) {
        (void)(tc_fail_types(12, fk, ak));
        return 1;
      } else {
      }
    } else {
    }
    arg = node_next[arg];
    field = node_next[field];
  }
  if ((arg != 0) || (field != 0)) {
    (void)(tc_fail(13));
    return 1;
  } else {
  }
  tc_kind = TY_NAMED;
  tc_name = variant_enum;
  tc_elem_kind = 0;
  tc_elem_name = 0;
  tc_result_type = ast_node(TY_NAMED, 0, 0, 0, variant_enum, 0);
  node_aux[id] = tc_result_type;
  return 1;
}
void tc_check_closure_escape(int id) {
  if (id == 0)
    return;
  else {
  }
  int cap = node_a[id];
  while (cap != 0) {
    if (node_aux[cap] != 1) {
      if (node_c[cap] < tc_global_count) {
      } else if ((node_c[cap] < tc_var_count) && (tc_var_param[node_c[cap]] == 1)) {
      } else
        (void)(tc_fail(60));
    } else {
    }
    cap = node_next[cap];
  }
}
void tc_check_closure_value_escape(int id) {
  if (id == 0)
    return;
  else {
  }
  if (node_kind[id] == N_CLOSURE) {
    (void)(tc_check_closure_escape(id));
    return;
  } else {
  }
  if (node_kind[id] == N_VAR) {
    if (tc_lookup_var(node_value[id]) == 1) {
      int cap = tc_var_closure_caps[tc_last_var_index];
      while (cap != 0) {
        if (node_aux[cap] != 1) {
          if (node_c[cap] < tc_global_count) {
          } else if ((node_c[cap] < tc_var_count) && (tc_var_param[node_c[cap]] == 1)) {
          } else
            (void)(tc_fail(60));
        } else {
        }
        cap = node_next[cap];
      }
    } else {
    }
  } else {
  }
}
void tc_attach_closure_caps(int destination, int caps) {
  if (destination < 0)
    return;
  else {
  }
  if (destination >= tc_var_count)
    return;
  else {
  }
  tc_var_closure_caps[destination] = caps;
  int cap = caps;
  while (cap != 0) {
    int cap_source = node_c[cap];
    if (cap_source >= 0) {
      if (cap_source < tc_var_count) {
        if (node_aux[cap] == 3)
          tc_var_borrow_mut[cap_source] = (tc_var_borrow_mut[cap_source] + 1);
        else if (node_aux[cap] == 2)
          tc_var_borrow_count[cap_source] = (tc_var_borrow_count[cap_source] + 1);
        else {
        }
      } else {
      }
    } else {
    }
    cap = node_next[cap];
  }
}
void tc_expr(int id) {
  tc_kind = TY_INT;
  tc_name = 0;
  tc_elem_kind = 0;
  tc_elem_name = 0;
  tc_result_type = 0;
  tc_expr_borrow_source = (0 - 1);
  tc_expr_borrow_origin = (0 - 1);
  tc_expr_borrow_mut = 0;
  tc_expr_borrow_param = 0;
  tc_expr_owner_source = (0 - 1);
  tc_expr_is_owned = 0;
  tc_expr_fun_target = 0;
  tc_expr_ffi_borrowed = 0;
  if ((id != 0) && (tc_ok == 1))
    tc_error_pos = node_pos[id];
  else {
  }
  if (id == 0) {
    (void)(tc_fail(4));
    return;
  } else {
  }
  int k = node_kind[id];
  if (k == N_INT) {
    tc_kind = TY_INT;
    tc_result_type = ast_node(TY_INT, 0, 0, 0, 0, 0);
    return;
  } else {
  }
  if (k == N_FLOAT) {
    if (node_aux[id] == TY_FLOAT) {
      tc_kind = TY_FLOAT;
      tc_result_type = ast_node(TY_FLOAT, 0, 0, 0, 0, 0);
    } else {
      tc_kind = TY_DOUBLE;
      tc_result_type = ast_node(TY_DOUBLE, 0, 0, 0, 0, 0);
    }
    return;
  } else {
  }
  if (k == N_CHAR) {
    tc_kind = TY_CHAR;
    tc_result_type = ast_node(TY_CHAR, 0, 0, 0, 0, 0);
    return;
  } else {
  }
  if (k == N_NULL) {
    tc_kind = TY_PTR;
    tc_name = 0;
    tc_elem_kind = TY_VOID;
    tc_elem_name = 0;
    tc_result_type = ast_node(TY_PTR, ast_node(TY_VOID, 0, 0, 0, 0, 0), 0, 0, 0, 0);
    return;
  } else {
  }
  if (k == N_BOOL) {
    tc_kind = TY_BOOL;
    tc_result_type = ast_node(TY_BOOL, 0, 0, 0, 0, 0);
    return;
  } else {
  }
  if (k == N_MOVE) {
    (void)(tc_expr(node_a[id]));
    int moved_kind = tc_kind;
    if (node_kind[node_a[id]] == N_VAR)
      (void)(tc_move_value(node_a[id]));
    else if (tc_expr_borrow_source >= 0)
      (void)(tc_fail(38));
    else if (tc_is_owner_kind(moved_kind) == 0)
      (void)(tc_fail(35));
    else {
    }
    tc_expr_borrow_source = (0 - 1);
    tc_expr_owner_source = (0 - 1);
    tc_expr_is_owned = 1;
    return;
  } else {
  }
  if (k == N_CLOSURE) {
    int saved_allow_outer_shadow = tc_allow_outer_shadow;
    (void)(tc_enter_scope());
    tc_allow_outer_shadow = 1;
    int cap = node_a[id];
    while (cap != 0) {
      int cap_name = node_a[cap];
      if (tc_lookup_var(cap_name) == 0)
        (void)(tc_fail(5));
      else {
        int source_index = tc_last_var_index;
        int source_type = tc_last_var_type;
        node_b[cap] = source_type;
        node_c[cap] = source_index;
        if (node_aux[cap] == 1) {
          if (tc_is_owner_type(source_type) == 0)
            (void)(tc_fail(58));
          else if (tc_var_moved[source_index] == 1)
            (void)(tc_fail(61));
          else {
            (void)(tc_move_var(source_index));
            if (tc_var_moved[source_index] == 1)
              tc_var_closure_moved[source_index] = 1;
            else {
            }
          }
        } else if (node_aux[cap] == 2) {
          if (tc_var_moved[source_index] == 1)
            (void)(tc_fail(33));
          else if (tc_var_borrow_mut[source_index] > 0)
            (void)(tc_fail(37));
          else {
          }
        } else if (node_aux[cap] == 3) {
          if (tc_var_moved[source_index] == 1)
            (void)(tc_fail(33));
          else if ((tc_var_borrow_count[source_index] > 0) || (tc_var_borrow_mut[source_index] > 0))
            (void)(tc_fail(37));
          else {
          }
        } else {
        }
      }
      cap = node_next[cap];
    }
    int body_cap = node_a[id];
    while (body_cap != 0) {
      int body_cap_type = node_b[body_cap];
      (void)(tc_type_node(body_cap_type));
      int body_cap_kind = tc_kind;
      int body_cap_name = tc_name;
      int body_cap_elem_kind = tc_elem_kind;
      int body_cap_elem_name = tc_elem_name;
      (void)(tc_add_var(node_a[body_cap], body_cap_kind, body_cap_name, body_cap_elem_kind,
                        body_cap_elem_name, body_cap_type));
      tc_var_mode[tc_last_var_index] = node_aux[body_cap];
      if (node_aux[body_cap] == 1)
        tc_var_owned[tc_last_var_index] = 1;
      else {
        tc_var_borrow_source[tc_last_var_index] = node_c[body_cap];
        tc_var_borrow_parent[tc_last_var_index] = node_c[body_cap];
        if (node_aux[body_cap] == 3) {
          tc_var_borrow_mut[tc_last_var_index] = 1;
          if (node_c[body_cap] < tc_var_count)
            tc_var_borrow_mut[node_c[body_cap]] = (tc_var_borrow_mut[node_c[body_cap]] + 1);
          else {
          }
        } else {
          tc_var_borrow_count[tc_last_var_index] = 1;
          if (node_c[body_cap] < tc_var_count)
            tc_var_borrow_count[node_c[body_cap]] = (tc_var_borrow_count[node_c[body_cap]] + 1);
          else {
          }
        }
      }
      body_cap = node_next[body_cap];
    }
    tc_allow_outer_shadow = saved_allow_outer_shadow;
    int p = node_c[id];
    int param_pos = 1;
    while (p != 0) {
      (void)(tc_type_node(node_b[p]));
      int pk = tc_kind;
      int pn = tc_name;
      int pek = tc_elem_kind;
      int pen = tc_elem_name;
      (void)(tc_add_var(node_a[p], pk, pn, pek, pen, node_b[p]));
      tc_var_param[tc_last_var_index] = 1;
      tc_var_param_pos[tc_last_var_index] = param_pos;
      tc_var_mode[tc_last_var_index] = node_aux[p];
      if ((node_aux[p] == 2) || (node_aux[p] == 3)) {
        tc_var_borrow_source[tc_last_var_index] = tc_last_var_index;
        tc_var_borrow_mode[tc_last_var_index] = node_aux[p];
        tc_var_borrow_parent[tc_last_var_index] = tc_last_var_index;
        tc_var_borrow_param[tc_last_var_index] = param_pos;
      } else {
      }
      if (node_aux[p] == 1)
        tc_var_owned[tc_last_var_index] = 1;
      else if (node_aux[p] == 3)
        tc_var_borrow_mut[tc_last_var_index] = 1;
      else if (tc_is_owner_kind(pk) == 1)
        tc_var_owned[tc_last_var_index] = 1;
      else {
      }
      param_pos = (param_pos + 1);
      p = node_next[p];
    }
    int saved_expected_elem_kind = tc_expected_elem_kind;
    int saved_expected_elem_name = tc_expected_elem_name;
    (void)(tc_type_node(node_value[id]));
    int ret_kind = tc_kind;
    int ret_name = tc_name;
    tc_expected_elem_kind = tc_elem_kind;
    tc_expected_elem_name = tc_elem_name;
    (void)(tc_stmt(node_b[id], ret_kind, ret_name));
    tc_expected_elem_kind = saved_expected_elem_kind;
    tc_expected_elem_name = saved_expected_elem_name;
    (void)(tc_leave_scope());
    int sig_args = 0;
    int sp = node_c[id];
    while (sp != 0) {
      int src = node_b[sp];
      int q = ast_node(node_kind[src], node_a[src], node_b[src], node_c[src], node_value[src],
                       node_aux[src]);
      if (sig_args == 0)
        sig_args = q;
      else
        sig_args = ast_link(sig_args, q);
      sp = node_next[sp];
    }
    int sig_ty = ast_node(TY_CLOSURE, sig_args, node_value[id], 0, 0, 0);
    tc_kind = TY_CLOSURE;
    tc_name = sig_ty;
    tc_elem_kind = 0;
    tc_elem_name = 0;
    tc_result_type = sig_ty;
    tc_expr_is_owned = 1;
    return;
  } else {
  }
  if (k == N_UNARY) {
    (void)(tc_expr(node_a[id]));
    if (tc_is_integer_kind(tc_kind) == 0) {
      (void)(tc_fail(15));
      return;
    } else {
    }
    tc_kind = TY_BOOL;
    tc_name = 0;
    tc_elem_kind = 0;
    tc_elem_name = 0;
    tc_result_type = ast_node(TY_BOOL, 0, 0, 0, 0, 0);
    return;
  } else {
  }
  if (k == N_STRING) {
    tc_kind = TY_STRING;
    tc_result_type = ast_node(TY_STRING, 0, 0, 0, 0, 0);
    return;
  } else {
  }
  if (k == N_VARIANT) {
    (void)(tc_check_variant(id));
    return;
  } else {
  }
  if (k == N_TUPLE) {
    int item = node_a[id];
    int types = 0;
    while (item != 0) {
      (void)(tc_expr(item));
      int elem_ty = tc_result_type;
      if (elem_ty == 0)
        elem_ty = tc_type_node_from_summary(tc_kind, tc_name, tc_elem_kind, tc_elem_name);
      else {
      }
      if (elem_ty == 0) {
        (void)(tc_fail(19));
        return;
      } else {
      }
      if (types == 0)
        types = elem_ty;
      else
        types = ast_link(types, elem_ty);
      item = node_next[item];
    }
    int tuple_ty = ast_node(TY_TUPLE, types, 0, 0, 0, 0);
    tc_kind = TY_TUPLE;
    tc_name = tuple_ty;
    tc_elem_kind = 0;
    tc_elem_name = 0;
    tc_result_type = tuple_ty;
    node_aux[id] = tuple_ty;
    return;
  } else {
  }
  if (k == N_VAR) {
    if (tc_lookup_var(node_value[id]) == 1) {
      if (tc_last_var_moved == 1) {
        if (tc_var_closure_moved[tc_last_var_index] == 1)
          (void)(tc_fail(61));
        else
          (void)(tc_fail(33));
      } else {
      }
      tc_expr_fun_target = tc_last_var_fun_target;
      tc_expr_borrow_source = tc_var_borrow_source[tc_last_var_index];
      if ((tc_expr_borrow_source < 0) && (tc_var_borrow_param[tc_last_var_index] > 0))
        tc_expr_borrow_source = tc_last_var_index;
      else {
      }
      tc_expr_borrow_mut = 0;
      if (tc_var_borrow_mode[tc_last_var_index] == 3)
        tc_expr_borrow_mut = 1;
      else {
      }
      tc_expr_borrow_param = tc_var_borrow_param[tc_last_var_index];
      tc_expr_ffi_borrowed = tc_last_var_ffi_borrowed;
      if (tc_last_var_owned == 1)
        tc_expr_owner_source = tc_last_var_index;
      else {
      }
      tc_expr_is_owned = 0;
      tc_result_type = tc_last_var_type;
      node_type[id] = tc_last_var_type;
      node_aux[id] = tc_last_var_type;
      return;
    } else {
    }
    int e = tc_find_enum_value(node_value[id]);
    if (e != 0) {
      tc_kind = TY_NAMED;
      tc_name = e;
      tc_result_type = ast_node(TY_NAMED, 0, 0, 0, e, 0);
      node_aux[id] = tc_result_type;
      return;
    } else {
    }
    tc_error_symbol = node_value[id];
    (void)(tc_fail(5));
    return;
  } else {
  }
  if (k == N_ADDRESS) {
    int address_entry = 0;
    if (node_kind[node_a[id]] == N_VAR)
      address_entry = tc_find_function_ctx(node_value[node_a[id]], node_scope[node_a[id]]);
    else {
    }
    if (address_entry != 0) {
      tc_kind = TY_FUN;
      tc_name = 0;
      tc_expr_fun_target = address_entry;
      tc_result_type = tc_signature_type(address_entry);
      return;
    } else {
    }
    if (tc_is_place(node_a[id]) == 0) {
      (void)(tc_fail(68));
      return;
    } else {
    }
    if ((node_value[id] == 1) && (tc_place_is_const(node_a[id]) == 1)) {
      (void)(tc_fail(70));
      return;
    } else {
    }
    (void)(tc_expr(node_a[id]));
    int oldk = tc_kind;
    int oldn = tc_name;
    int olde = tc_elem_kind;
    int olden = tc_elem_name;
    int old_type_node = tc_result_type;
    int old_source = tc_expr_borrow_source;
    int old_origin = tc_expr_borrow_origin;
    int old_mut = tc_expr_borrow_mut;
    int old_param = tc_expr_borrow_param;
    int root = old_source;
    if (root < 0)
      root = tc_place_root(node_a[id]);
    else {
    }
    if (old_origin < 0)
      old_origin = root;
    else {
    }
    if ((root < 0) || (root >= tc_var_count)) {
      (void)(tc_fail(68));
      return;
    } else {
    }
    if (node_value[id] == 1) {
      if ((old_mut == 0) && (tc_var_mode[root] == 2)) {
        (void)(tc_fail(70));
        return;
      } else {
      }
      if (((((old_mut == 0) && (old_origin >= 0)) && (old_origin < tc_var_count)) &&
           (tc_var_borrow_mode[old_origin] == 2)) &&
          (tc_var_param[old_origin] == 1)) {
        (void)(tc_fail(37));
        return;
      } else {
      }
      if (tc_borrow_conflict_from(root, old_origin, 1) == 1) {
        (void)(tc_fail(37));
        return;
      } else {
      }
      tc_expr_borrow_mut = 1;
    } else {
      if (tc_borrow_conflict_from(root, old_origin, 0) == 1) {
        (void)(tc_fail(37));
        return;
      } else {
      }
      tc_expr_borrow_mut = 0;
    }
    tc_expr_borrow_source = root;
    tc_expr_borrow_origin = old_origin;
    tc_expr_borrow_param = old_param;
    if ((tc_var_param[root] == 1) && ((tc_var_mode[root] == 2) || (tc_var_mode[root] == 3)))
      tc_expr_borrow_param = tc_var_param_pos[root];
    else {
    }
    tc_kind = TY_PTR;
    tc_name = 0;
    tc_elem_kind = oldk;
    tc_elem_name = oldn;
    if (old_type_node != 0) {
      tc_elem_kind = node_kind[old_type_node];
      if (tc_elem_kind == TY_NAMED)
        tc_elem_name = node_value[old_type_node];
      else if ((tc_elem_kind == TY_PARAM) || (tc_type_is_structural(tc_elem_kind) == 1))
        tc_elem_name = old_type_node;
      else
        tc_elem_name = 0;
    } else if (oldk == TY_PTR) {
      tc_elem_kind = olde;
      tc_elem_name = olden;
    } else {
    }
    tc_expr_owner_source = (0 - 1);
    tc_expr_is_owned = 0;
    int pointee = old_type_node;
    if (pointee == 0)
      pointee = tc_type_node_from_summary(oldk, oldn, olde, olden);
    else {
    }
    tc_result_type = ast_node(TY_PTR, pointee, 0, 0, 0, 0);
    return;
  } else {
  }
  if (k == N_DEREF) {
    (void)(tc_expr(node_a[id]));
    int deref_borrow = tc_expr_borrow_source;
    int deref_origin = tc_expr_borrow_origin;
    int deref_mut = tc_expr_borrow_mut;
    int deref_param = tc_expr_borrow_param;
    int deref_type_node = tc_result_type;
    if (tc_kind != TY_PTR)
      (void)(tc_fail(6));
    else {
      if (((deref_type_node != 0) && (node_kind[deref_type_node] == TY_PTR)) &&
          (node_a[deref_type_node] != 0))
        (void)(tc_type_node(node_a[deref_type_node]));
      else {
        tc_kind = tc_elem_kind;
        tc_name = tc_elem_name;
        tc_elem_kind = 0;
        tc_elem_name = 0;
        tc_result_type = tc_type_node_from_summary(tc_kind, tc_name, tc_elem_kind, tc_elem_name);
      }
      tc_expr_borrow_source = deref_borrow;
      tc_expr_borrow_origin = deref_origin;
      tc_expr_borrow_mut = deref_mut;
      tc_expr_borrow_param = deref_param;
    }
    node_type[id] = tc_result_type;
    return;
  } else {
  }
  if (k == N_INDEX) {
    (void)(tc_expr(node_b[id]));
    if (tc_kind != TY_INT)
      (void)(tc_fail(7));
    else {
    }
    (void)(tc_expr(node_a[id]));
    int index_borrow = tc_expr_borrow_source;
    int index_origin = tc_expr_borrow_origin;
    int index_mut = tc_expr_borrow_mut;
    int index_param = tc_expr_borrow_param;
    int index_container_type = tc_result_type;
    if (tc_kind == TY_ARRAY) {
      int ix = node_b[id];
      int ik = (0 - 1);
      int is_const = 0;
      if (node_kind[ix] == N_INT) {
        ik = node_value[ix];
        is_const = 1;
      } else if (((((node_kind[ix] == N_BINOP) && (node_value[ix] == OP_SUB)) &&
                   (node_kind[node_a[ix]] == N_INT)) &&
                  (node_value[node_a[ix]] == 0)) &&
                 (node_kind[node_b[ix]] == N_INT)) {
        ik = (0 - node_value[node_b[ix]]);
        is_const = 1;
      } else {
      }
      if ((((is_const == 1) && (index_container_type != 0)) &&
           (node_kind[index_container_type] == TY_ARRAY)) &&
          ((ik < 0) || (ik > (node_value[index_container_type] - 1))))
        (void)(tc_fail(45));
      else {
      }
      if (((index_container_type != 0) && (node_kind[index_container_type] == TY_ARRAY)) &&
          (node_a[index_container_type] != 0))
        (void)(tc_type_node(node_a[index_container_type]));
      else {
        tc_kind = TY_INT;
        tc_name = 0;
        tc_elem_kind = 0;
        tc_elem_name = 0;
        tc_result_type = tc_type_node_from_summary(tc_kind, tc_name, tc_elem_kind, tc_elem_name);
      }
      tc_expr_borrow_source = index_borrow;
      tc_expr_borrow_origin = index_origin;
      tc_expr_borrow_mut = index_mut;
      tc_expr_borrow_param = index_param;
    } else if ((tc_kind == TY_DYN_ARRAY) || (tc_kind == TY_PTR)) {
      if (((index_container_type != 0) && (node_kind[index_container_type] == tc_kind)) &&
          (node_a[index_container_type] != 0))
        (void)(tc_type_node(node_a[index_container_type]));
      else {
        int ek = tc_elem_kind;
        int en = tc_elem_name;
        tc_kind = ek;
        tc_name = en;
        tc_elem_kind = 0;
        tc_elem_name = 0;
        tc_result_type = tc_type_node_from_summary(tc_kind, tc_name, tc_elem_kind, tc_elem_name);
      }
      tc_expr_borrow_source = index_borrow;
      tc_expr_borrow_origin = index_origin;
      tc_expr_borrow_mut = index_mut;
      tc_expr_borrow_param = index_param;
    } else if (tc_kind == TY_STRING) {
      tc_kind = TY_CHAR;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_expr_borrow_source = (0 - 1);
      tc_expr_borrow_origin = (0 - 1);
      tc_expr_borrow_mut = 0;
      tc_expr_borrow_param = 0;
    } else
      (void)(tc_fail(8));
    tc_result_type = tc_type_node_from_summary(tc_kind, tc_name, tc_elem_kind, tc_elem_name);
    node_type[id] = tc_result_type;
    return;
  } else {
  }
  if (k == N_FIELD_ACCESS) {
    (void)(tc_expr(node_a[id]));
    int base_borrow = tc_expr_borrow_source;
    int base_origin = tc_expr_borrow_origin;
    int base_mut = tc_expr_borrow_mut;
    int base_param = tc_expr_borrow_param;
    int base_kind = tc_kind;
    int base_name = tc_name;
    int field_target_root = tc_field_target_root_for_place(node_a[id]);
    tc_expr_fun_target = tc_field_target_get(field_target_root, node_value[id]);
    if (base_kind == TY_DYN_ARRAY) {
      if (bi_has_flag(node_value[id], BI_FLAG_DYNFIELD) == 1) {
        tc_kind = TY_INT;
        tc_name = 0;
        tc_elem_kind = 0;
        tc_elem_name = 0;
        tc_expr_borrow_source = base_borrow;
        tc_expr_borrow_origin = base_origin;
        tc_expr_borrow_mut = base_mut;
        tc_expr_borrow_param = base_param;
        return;
      } else {
      }
      (void)(tc_fail(11));
      return;
    } else {
    }
    if (base_kind == TY_PTR) {
      if (tc_elem_kind == TY_GENERIC) {
        base_kind = TY_GENERIC;
        base_name = tc_elem_name;
      } else {
        base_kind = TY_NAMED;
        base_name = tc_elem_name;
      }
    } else {
    }
    if (base_kind == TY_VARIANT) {
      int variant = base_name;
      int payload_field = node_b[variant];
      while (payload_field != 0) {
        if (node_a[payload_field] == node_value[id]) {
          (void)(tc_type_node(node_b[payload_field]));
          tc_expr_borrow_source = base_borrow;
          tc_expr_borrow_origin = base_origin;
          tc_expr_borrow_mut = base_mut;
          tc_expr_borrow_param = base_param;
          return;
        } else {
        }
        payload_field = node_next[payload_field];
      }
      (void)(tc_fail(11));
      return;
    } else {
    }
    if (base_kind == TY_NAMED) {
      if ((node_value[id] == sym_tag_id()) && (tc_find_enum(base_name) != 0)) {
        tc_kind = TY_INT;
        tc_name = 0;
        tc_elem_kind = 0;
        tc_elem_name = 0;
        tc_expr_borrow_source = base_borrow;
        tc_expr_borrow_origin = base_origin;
        tc_expr_borrow_mut = base_mut;
        tc_expr_borrow_param = base_param;
        return;
      } else {
      }
      int enum_decl = tc_find_enum(base_name);
      if (enum_decl != 0) {
        int variant_item = node_a[enum_decl];
        while (variant_item != 0) {
          if (node_a[variant_item] == node_value[id]) {
            if (node_b[variant_item] == 0) {
              (void)(tc_fail(11));
              return;
            } else {
            }
            tc_kind = TY_VARIANT;
            tc_name = variant_item;
            tc_elem_kind = 0;
            tc_elem_name = 0;
            tc_result_type = ast_node(TY_VARIANT, 0, 0, 0, variant_item, base_name);
            node_aux[id] = tc_result_type;
            tc_expr_borrow_source = base_borrow;
            tc_expr_borrow_origin = base_origin;
            tc_expr_borrow_mut = base_mut;
            tc_expr_borrow_param = base_param;
            return;
          } else {
          }
          variant_item = node_next[variant_item];
        }
      } else {
      }
    } else {
    }
    if (base_kind == TY_GENERIC) {
      int base_ty = base_name;
      int sgen = tc_find_struct(node_value[base_ty]);
      if (sgen == 0) {
        (void)(tc_fail(10));
        return;
      } else {
      }
      (void)(tc_bind_push());
      (void)(tc_bind_clear());
      int gp = node_c[sgen];
      int ga = node_a[base_ty];
      while ((gp != 0) && (ga != 0)) {
        if (tc_bind_add(node_a[gp], ga) == 0) {
          (void)(tc_bind_pop());
          return;
        } else {
        }
        gp = node_next[gp];
        ga = node_next[ga];
      }
      int gf = node_a[sgen];
      while (gf != 0) {
        if (node_a[gf] == node_value[id]) {
          int subst = tc_substitute_type(node_b[gf]);
          (void)(tc_type_node(subst));
          (void)(tc_bind_pop());
          tc_expr_borrow_source = base_borrow;
          tc_expr_borrow_origin = base_origin;
          tc_expr_borrow_mut = base_mut;
          tc_expr_borrow_param = base_param;
          return;
        } else {
        }
        gf = node_next[gf];
      }
      (void)(tc_bind_pop());
      (void)(tc_fail(11));
      return;
    } else {
    }
    if (base_kind != TY_NAMED) {
      (void)(tc_fail(9));
      return;
    } else {
    }
    int s = tc_find_struct(base_name);
    if (s == 0) {
      (void)(tc_fail(10));
      return;
    } else {
    }
    int f = node_a[s];
    while (f != 0) {
      if (node_a[f] == node_value[id]) {
        (void)(tc_type_node(node_b[f]));
        tc_expr_borrow_source = base_borrow;
        tc_expr_borrow_origin = base_origin;
        tc_expr_borrow_mut = base_mut;
        tc_expr_borrow_param = base_param;
        return;
      } else {
      }
      f = node_next[f];
    }
    (void)(tc_fail(11));
    return;
  } else {
  }
  if (k == N_CALL) {
    if (tc_check_variant(id) == 1) {
      node_kind[id] = N_VARIANT;
      return;
    } else {
    }
    int call_name = node_value[id];
    int btag = bi_tag(call_name);
    if (bi_has_flag(call_name, BI_FLAG_OWNED) == 1)
      tc_expr_is_owned = 1;
    else {
    }
    if (btag == BI_TC_MEM_ALLOC) {
      int aa = node_a[id];
      if (((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      int witness = node_next[aa];
      (void)(tc_expr(witness));
      if (tc_kind == TY_VOID) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      int wk = tc_kind;
      int wn = tc_name;
      int witness_ty = tc_result_type;
      if (witness_ty == 0)
        witness_ty = tc_type_node_from_summary(wk, wn, tc_elem_kind, tc_elem_name);
      else {
      }
      tc_kind = TY_PTR;
      tc_name = 0;
      tc_elem_kind = wk;
      tc_elem_name = wn;
      tc_result_type = ast_node(TY_PTR, witness_ty, 0, 0, 0, 0);
      return;
    } else {
    }
    if (btag == BI_TC_MEM_ALLOC_ALIGNED) {
      int aa = node_a[id];
      if ((((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] == 0)) ||
          (node_next[node_next[node_next[aa]]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      if ((node_kind[aa] == N_INT) && (node_value[aa] < 1)) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind == TY_VOID) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      int wk = tc_kind;
      int wn = tc_name;
      int witness_ty = tc_result_type;
      if (witness_ty == 0)
        witness_ty = tc_type_node_from_summary(wk, wn, tc_elem_kind, tc_elem_name);
      else {
      }
      tc_kind = TY_PTR;
      tc_name = 0;
      tc_elem_kind = wk;
      tc_elem_name = wn;
      tc_result_type = ast_node(TY_PTR, witness_ty, 0, 0, 0, 0);
      return;
    } else {
    }
    if (btag == BI_TC_MEM_RESIZE) {
      int aa = node_a[id];
      if (((((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] == 0)) ||
           (node_next[node_next[node_next[aa]]] == 0)) ||
          (node_next[node_next[node_next[node_next[aa]]]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_PTR) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      int pk = tc_elem_kind;
      int pn = tc_elem_name;
      int ptr_ty = tc_result_type;
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if ((tc_kind == TY_VOID) || (tc_array_elem_same(pk, pn, tc_kind, tc_name) == 0)) {
        (void)(tc_fail_types(36, pk, tc_kind));
        return;
      } else {
      }
      tc_kind = TY_PTR;
      tc_name = 0;
      tc_elem_kind = pk;
      tc_elem_name = pn;
      tc_result_type = ptr_ty;
      return;
    } else {
    }
    if (btag == BI_TC_MEM_FREE) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_PTR) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      if (tc_expr_ffi_borrowed == 1) {
        (void)(tc_fail(67));
        return;
      } else {
      }
      tc_kind = TY_VOID;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = 0;
      return;
    } else {
    }
    if (btag == BI_TC_SYS_COMPILE) {
      int aa = node_a[id];
      if (((((((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] == 0)) ||
             (node_next[node_next[node_next[aa]]] == 0)) ||
            (node_next[node_next[node_next[node_next[aa]]]] == 0)) ||
           (node_next[node_next[node_next[node_next[node_next[aa]]]]] == 0)) ||
          (node_next[node_next[node_next[node_next[node_next[node_next[aa]]]]]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_STRING) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_STRING) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_STRING) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_PTR) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_INT;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = ast_node(TY_INT, 0, 0, 0, 0, 0);
      return;
    } else {
    }
    if (btag == BI_TC_SYS_RUN) {
      int aa = node_a[id];
      if (((((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] == 0)) ||
           (node_next[node_next[node_next[aa]]] == 0)) ||
          (node_next[node_next[node_next[node_next[aa]]]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_STRING) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_PTR) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_INT;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = ast_node(TY_INT, 0, 0, 0, 0, 0);
      return;
    } else {
    }
    if ((btag == BI_TC_SYS_STRING) || (btag == BI_TC_SYS_INT)) {
      if (node_a[id] != 0) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      if (btag == BI_TC_SYS_STRING)
        tc_kind = TY_STRING;
      else
        tc_kind = TY_INT;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      if (btag == BI_TC_SYS_STRING)
        tc_result_type = ast_node(TY_STRING, 0, 0, 0, 0, 0);
      else
        tc_result_type = ast_node(TY_INT, 0, 0, 0, 0, 0);
      return;
    } else {
    }
    if (btag == BI_TC_READ_LINE) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_STRING;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      return;
    } else {
    }
    if (btag == BI_TC_READ_INT) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_INT;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      return;
    } else {
    }
    if ((btag == BI_TC_WRITE_STRING) || (btag == BI_TC_WRITE_LINE)) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_STRING) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_VOID;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      return;
    } else {
    }
    if (btag == BI_TC_WRITE_INT) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_VOID;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      return;
    } else {
    }
    if (btag == BI_TC_WRITE_CHAR) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_CHAR) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_VOID;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      return;
    } else {
    }
    if (btag == BI_TC_IO_STATUS) {
      if (node_a[id] != 0) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      tc_kind = TY_INT;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      return;
    } else {
    }
    if (btag == BI_TC_ATOMIC_MAKE) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_PTR;
      tc_name = 0;
      tc_elem_kind = TY_VOID;
      tc_elem_name = 0;
      tc_result_type = ast_node(TY_PTR, ast_node(TY_VOID, 0, 0, 0, 0, 0), 0, 0, 0, 0);
      return;
    } else {
    }
    if ((btag == BI_TC_ATOMIC_LOAD) || (btag == BI_TC_ATOMIC_FREE)) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if ((tc_kind != TY_PTR) || (tc_elem_kind != TY_VOID)) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      if (btag == BI_TC_ATOMIC_FREE) {
        tc_kind = TY_VOID;
        tc_name = 0;
        tc_elem_kind = 0;
        tc_elem_name = 0;
        tc_result_type = 0;
      } else {
        tc_kind = TY_INT;
        tc_name = 0;
        tc_elem_kind = 0;
        tc_elem_name = 0;
        tc_result_type = ast_node(TY_INT, 0, 0, 0, 0, 0);
      }
      return;
    } else {
    }
    if (btag == BI_TC_ATOMIC_FETCH_ADD) {
      int aa = node_a[id];
      if (((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if ((tc_kind != TY_PTR) || (tc_elem_kind != TY_VOID)) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_INT;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = ast_node(TY_INT, 0, 0, 0, 0, 0);
      return;
    } else {
    }
    if (btag == BI_TC_ATOMIC_STORE) {
      int aa = node_a[id];
      if (((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if ((tc_kind != TY_PTR) || (tc_elem_kind != TY_VOID)) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_VOID;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = 0;
      return;
    } else {
    }
    if (btag == BI_TC_ATOMIC_CAS) {
      int aa = node_a[id];
      if ((((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] == 0)) ||
          (node_next[node_next[node_next[aa]]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if ((tc_kind != TY_PTR) || (tc_elem_kind != TY_VOID)) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_INT;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = ast_node(TY_INT, 0, 0, 0, 0, 0);
      return;
    } else {
    }
    if (btag == BI_TC_CHANNEL_MAKE) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if (tc_kind != TY_INT) {
        (void)(tc_fail(17));
        return;
      } else {
      }
      tc_kind = TY_PTR;
      tc_name = 0;
      tc_elem_kind = TY_VOID;
      tc_elem_name = 0;
      tc_result_type = ast_node(TY_PTR, ast_node(TY_VOID, 0, 0, 0, 0, 0), 0, 0, 0, 0);
      return;
    } else {
    }
    if ((btag == BI_TC_CHANNEL_SEND) || (btag == BI_TC_CHANNEL_RECV)) {
      int aa = node_a[id];
      if (((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if ((tc_kind != TY_PTR) || (tc_elem_kind != TY_VOID)) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      aa = node_next[aa];
      (void)(tc_expr(aa));
      if (btag == BI_TC_CHANNEL_SEND) {
        if (tc_kind != TY_INT) {
          (void)(tc_fail(17));
          return;
        } else {
        }
      } else if ((tc_kind != TY_PTR) || (tc_elem_kind != TY_INT)) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      tc_kind = TY_INT;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = ast_node(TY_INT, 0, 0, 0, 0, 0);
      return;
    } else {
    }
    if ((btag == BI_TC_CHANNEL_CLOSE) || (btag == BI_TC_CHANNEL_FREE)) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if ((tc_kind != TY_PTR) || (tc_elem_kind != TY_VOID)) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      tc_kind = TY_VOID;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = 0;
      return;
    } else {
    }
    if (btag == BI_TC_THREAD_SPAWN) {
      int aa = node_a[id];
      if (((aa == 0) || (node_next[aa] == 0)) || (node_next[node_next[aa]] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      int callback_ok = 0;
      if ((node_kind[aa] == N_ADDRESS) && (node_kind[node_a[aa]] == N_VAR)) {
        int entry = tc_find_function_ctx(node_value[node_a[aa]], node_scope[node_a[aa]]);
        if (((entry != 0) && (node_b[entry] != 0)) && (node_kind[node_b[entry]] == TY_INT)) {
          int ep = node_c[entry];
          if (((((ep != 0) && (node_next[ep] == 0)) && (node_kind[node_b[ep]] == TY_PTR)) &&
               (node_a[node_b[ep]] != 0)) &&
              (node_kind[node_a[node_b[ep]]] == TY_VOID))
            callback_ok = 1;
          else {
          }
        } else {
        }
      } else {
        (void)(tc_expr(aa));
        int callback_ty = tc_result_type;
        if (((((tc_kind == TY_FUN) && (callback_ty != 0)) && (node_kind[callback_ty] == TY_FUN)) &&
             (node_b[callback_ty] != 0)) &&
            (node_kind[node_b[callback_ty]] == TY_INT)) {
          int ep = node_a[callback_ty];
          if (((((ep != 0) && (node_next[ep] == 0)) && (node_kind[ep] == TY_PTR)) &&
               (node_a[ep] != 0)) &&
              (node_kind[node_a[ep]] == TY_VOID))
            callback_ok = 1;
          else {
          }
        } else {
        }
      }
      if (callback_ok == 0) {
        (void)(tc_fail_types(12, TY_FUN, tc_kind));
        return;
      } else {
      }
      int arg = node_next[aa];
      (void)(tc_expr(arg));
      if ((tc_kind != TY_PTR) || (tc_elem_kind != TY_VOID)) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      tc_kind = TY_PTR;
      tc_name = 0;
      tc_elem_kind = TY_VOID;
      tc_elem_name = 0;
      tc_result_type = ast_node(TY_PTR, ast_node(TY_VOID, 0, 0, 0, 0, 0), 0, 0, 0, 0);
      return;
    } else {
    }
    if (btag == BI_TC_THREAD_JOIN) {
      int aa = node_a[id];
      if ((aa == 0) || (node_next[aa] != 0)) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      (void)(tc_expr(aa));
      if ((tc_kind != TY_PTR) || (tc_elem_kind != TY_VOID)) {
        (void)(tc_fail(8));
        return;
      } else {
      }
      tc_kind = TY_INT;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = ast_node(TY_INT, 0, 0, 0, 0, 0);
      return;
    } else {
    }
    if (btag == BI_TC_THREAD_YIELD) {
      if (node_a[id] != 0) {
        (void)(tc_fail(13));
        return;
      } else {
      }
      tc_kind = TY_VOID;
      tc_name = 0;
      tc_elem_kind = 0;
      tc_elem_name = 0;
      tc_result_type = 0;
      return;
    } else {
    }
    if ((node_b[id] != 0) && (btag != 0)) {
      (void)(tc_fail(76));
      return;
    } else {
    }
    int fun_node = tc_find_function_ctx(node_value[id], node_scope[id]);
    if (fun_node == 0) {
      if ((tc_lookup_var(node_value[id]) == 1) && (tc_kind == TY_FUN)) {
        int fty = tc_last_var_type;
        int call_target_fp = tc_last_var_fun_target;
        if ((fty == 0) || (node_kind[fty] != TY_FUN)) {
          (void)(tc_fail(42));
          return;
        } else {
        }
        if ((call_target_fp == 0) && (tc_fun_type_has_sensitive_param(fty) == 1)) {
          (void)(tc_fail(73));
          return;
        } else {
        }
        int arg_fp = node_a[id];
        int p_fp = node_a[fty];
        int target_param_fp = 0;
        if (call_target_fp != 0)
          target_param_fp = node_c[call_target_fp];
        else {
        }
        while ((arg_fp != 0) && (p_fp != 0)) {
          (void)(tc_expr(arg_fp));
          int ak_fp = tc_kind;
          int an_fp = tc_name;
          int aek_fp = tc_elem_kind;
          int aen_fp = tc_elem_name;
          (void)(tc_type_node(p_fp));
          int pek_fp = tc_kind;
          int pen_fp = tc_name;
          int peek_fp = tc_elem_kind;
          int peen_fp = tc_elem_name;
          if ((call_target_fp != 0) && (target_param_fp != 0))
            (void)(tc_check_call_borrow(arg_fp, node_aux[target_param_fp]));
          else {
          }
          if (tc_literal_fits(arg_fp, pek_fp) == 0)
            (void)(tc_fail(54));
          else {
          }
          if ((pek_fp == TY_DYN_ARRAY) && (ak_fp == TY_DYN_ARRAY))
            (void)(tc_move_value(arg_fp));
          else {
          }
          if (tc_same_full(ak_fp, an_fp, aek_fp, aen_fp, pek_fp, pen_fp, peek_fp, peen_fp) == 0)
            if (tc_integer_literal_conversion(arg_fp, pek_fp, ak_fp) == 0)
              (void)(tc_fail_types(12, pek_fp, ak_fp));
            else {
            }
          else {
          }
          arg_fp = node_next[arg_fp];
          p_fp = node_next[p_fp];
          if (target_param_fp != 0)
            target_param_fp = node_next[target_param_fp];
          else {
          }
        }
        if ((arg_fp != 0) || (p_fp != 0))
          (void)(tc_fail(13));
        else {
        }
        int fp_ret_param = 0;
        int fp_ret_mut = 0;
        if (call_target_fp != 0) {
          (void)(ensure_tc_fun_meta(call_target_fp));
          fp_ret_param = tc_fun_return_param[call_target_fp];
          fp_ret_mut = tc_fun_return_mut[call_target_fp];
        } else {
        }
        int fp_ret_arg = tc_nth_arg(node_a[id], fp_ret_param);
        int fp_ret_source = tc_provenance(fp_ret_arg);
        int fp_ret_life = tc_provenance_param(fp_ret_arg);
        (void)(tc_type_node(node_b[fty]));
        if (fp_ret_param > 0) {
          tc_expr_borrow_source = fp_ret_source;
          tc_expr_borrow_mut = fp_ret_mut;
          tc_expr_borrow_param = fp_ret_life;
        } else {
        }
        return;
      } else {
      }
      if (btag == BI_TC_PTR_INT) {
        tc_kind = TY_PTR;
        tc_name = 0;
        tc_elem_kind = TY_INT;
        tc_elem_name = 0;
        return;
      } else {
      }
      if (btag == BI_TC_VOID) {
        tc_kind = TY_VOID;
        tc_name = 0;
        return;
      } else {
      }
      if (btag == BI_TC_PTR_VOID) {
        tc_kind = TY_PTR;
        tc_name = 0;
        tc_elem_kind = TY_VOID;
        tc_elem_name = 0;
        return;
      } else {
      }
      if (btag == BI_TC_INT) {
        tc_kind = TY_INT;
        tc_name = 0;
        return;
      } else {
      }
      if (btag == BI_TC_STRING) {
        tc_kind = TY_STRING;
        tc_name = 0;
        return;
      } else {
      }
      (void)(tc_fail(41));
      return;
    } else {
    }
    (void)(ensure_tc_fun_meta(fun_node));
    if ((node_b[id] != 0) && (node_kind[fun_node] != N_GENERIC_FUNC)) {
      (void)(tc_fail(76));
      return;
    } else {
    }
    if (node_kind[fun_node] == N_GENERIC_FUNC) {
      (void)(tc_bind_push());
      (void)(tc_bind_clear());
      if ((node_b[id] != 0) && (tc_bind_explicit_args(fun_node, node_b[id]) == 0)) {
        (void)(tc_bind_pop());
        return;
      } else {
      }
      int generic_moves_array = tc_generic_moves_array(fun_node);
      int ga = node_a[id];
      int gp = node_c[fun_node];
      while ((ga != 0) && (gp != 0)) {
        int formal_arg = node_b[gp];
        if (node_kind[formal_arg] == TY_PARAM) {
          int bound_arg = tc_bind_find(node_value[formal_arg]);
          if ((bound_arg != 0) && (node_kind[bound_arg] == TY_FLOAT))
            (void)(tc_mark_float_expr(ga, TY_FLOAT));
          else if ((bound_arg != 0) && (node_kind[bound_arg] == TY_DOUBLE))
            (void)(tc_mark_float_expr(ga, TY_DOUBLE));
          else {
          }
        } else {
        }
        (void)(tc_expr(ga));
        int actual_kind = tc_kind;
        int actual_ty = tc_result_type;
        (void)(tc_check_call_borrow(ga, node_aux[gp]));
        if (((generic_moves_array == 1) && (actual_kind == TY_DYN_ARRAY)) && (node_aux[gp] == 0))
          (void)(tc_move_value(ga));
        else {
        }
        if (actual_ty == 0)
          actual_ty = tc_type_node_from_summary(tc_kind, tc_name, tc_elem_kind, tc_elem_name);
        else {
        }
        (void)(tc_match_generic_call_arg(formal_arg, actual_ty, ga));
        ga = node_next[ga];
        gp = node_next[gp];
      }
      if ((ga != 0) || (gp != 0))
        (void)(tc_fail(13));
      else {
      }
      int generic_ret_param = tc_fun_return_param[fun_node];
      int generic_ret_arg = tc_nth_arg(node_a[id], generic_ret_param);
      int generic_ret_source = tc_provenance(generic_ret_arg);
      int generic_ret_mut = tc_fun_return_mut[fun_node];
      int generic_ret_life = tc_provenance_param(generic_ret_arg);
      int generic_ret = tc_substitute_type(node_b[fun_node]);
      (void)(tc_type_node(generic_ret));
      if (generic_ret_param > 0) {
        tc_expr_borrow_source = generic_ret_source;
        tc_expr_borrow_mut = generic_ret_mut;
        tc_expr_borrow_param = generic_ret_life;
      } else {
      }
      node_aux[id] = tc_result_type;
      (void)(tc_bind_pop());
      return;
    } else {
    }
    int arg = node_a[id];
    int p = node_c[fun_node];
    while ((arg != 0) && (p != 0)) {
      (void)(tc_type_node(node_b[p]));
      int pek = tc_kind;
      int pen = tc_name;
      int peek = tc_elem_kind;
      int peen = tc_elem_name;
      int formal_type = tc_result_type;
      (void)(tc_mark_float_expr(arg, pek));
      (void)(tc_expr(arg));
      int ak = tc_kind;
      int an = tc_name;
      int aek = tc_elem_kind;
      int aen = tc_elem_name;
      int actual_type = tc_result_type;
      (void)(tc_check_call_borrow(arg, node_aux[p]));
      if (tc_literal_fits(arg, pek) == 0)
        (void)(tc_fail(54));
      else {
      }
      if (((pek == TY_DYN_ARRAY) && (ak == TY_DYN_ARRAY)) && (node_aux[p] == 0))
        (void)(tc_move_value(arg));
      else {
      }
      if (tc_same_full(ak, an, aek, aen, pek, pen, peek, peen) == 0)
        if (tc_integer_literal_conversion(arg, pek, ak) == 0)
          (void)(tc_fail_types(12, pek, ak));
        else {
        }
      else {
      }
      if ((ak == TY_FUN) && (pek == TY_FUN)) {
        if (actual_type == 0)
          (void)(tc_fail_types(12, TY_FUN, 0));
        else {
        }
        if (formal_type == 0)
          (void)(tc_fail_types(12, 0, TY_FUN));
        else {
        }
        if (((actual_type != 0) && (formal_type != 0)) &&
            (tc_type_equal(actual_type, formal_type) == 0))
          (void)(tc_fail_types(12, node_kind[formal_type], node_kind[actual_type]));
        else {
        }
      } else {
      }
      arg = node_next[arg];
      p = node_next[p];
    }
    if ((arg != 0) || (p != 0))
      (void)(tc_fail(13));
    else {
    }
    int ret_param = tc_fun_return_param[fun_node];
    int ret_arg = tc_nth_arg(node_a[id], ret_param);
    int ret_source = tc_provenance(ret_arg);
    int ret_mut = tc_fun_return_mut[fun_node];
    int ret_life = tc_provenance_param(ret_arg);
    (void)(tc_type_node(node_b[fun_node]));
    if (ret_param > 0) {
      tc_expr_borrow_source = ret_source;
      tc_expr_borrow_mut = ret_mut;
      tc_expr_borrow_param = ret_life;
    } else {
    }
    if ((node_kind[fun_node] == N_EXTERN) && ((tc_kind == TY_PTR) || (tc_kind == TY_STRING)))
      tc_expr_ffi_borrowed = 1;
    else {
    }
    return;
  } else {
  }
  if (k == N_INDIRECT_CALL) {
    (void)(tc_expr(node_a[id]));
    int call_target = tc_expr_fun_target;
    if (((tc_kind == TY_CLOSURE) && (tc_result_type != 0)) &&
        (node_kind[tc_result_type] == TY_CLOSURE)) {
    } else if (((tc_kind != TY_FUN) || (tc_result_type == 0)) ||
               (node_kind[tc_result_type] != TY_FUN)) {
      (void)(tc_fail_types(12, TY_FUN, tc_kind));
      return;
    } else {
    }
    int fty = tc_result_type;
    if ((call_target == 0) && (tc_fun_type_has_sensitive_param(fty) == 1)) {
      (void)(tc_fail(73));
      return;
    } else {
    }
    int arg = node_b[id];
    int param = node_a[fty];
    int target_param = 0;
    if (call_target != 0)
      target_param = node_c[call_target];
    else {
    }
    while ((arg != 0) && (param != 0)) {
      (void)(tc_type_node(param));
      int pk = tc_kind;
      int pn = tc_name;
      int pek = tc_elem_kind;
      int pen = tc_elem_name;
      (void)(tc_mark_float_expr(arg, pk));
      (void)(tc_expr(arg));
      int ak = tc_kind;
      int an = tc_name;
      int aek = tc_elem_kind;
      int aen = tc_elem_name;
      if ((call_target != 0) && (target_param != 0))
        (void)(tc_check_call_borrow(arg, node_aux[target_param]));
      else {
      }
      if (tc_literal_fits(arg, pk) == 0)
        (void)(tc_fail(54));
      else {
      }
      if (tc_same_full(ak, an, aek, aen, pk, pn, pek, pen) == 0)
        if (tc_integer_literal_conversion(arg, pk, ak) == 0)
          (void)(tc_fail_types(12, pk, ak));
        else {
        }
      else {
      }
      arg = node_next[arg];
      param = node_next[param];
      if (target_param != 0)
        target_param = node_next[target_param];
      else {
      }
    }
    if ((arg != 0) || (param != 0)) {
      (void)(tc_fail(13));
      return;
    } else {
    }
    int indirect_ret_param = 0;
    int indirect_ret_mut = 0;
    if (call_target != 0) {
      (void)(ensure_tc_fun_meta(call_target));
      indirect_ret_param = tc_fun_return_param[call_target];
      indirect_ret_mut = tc_fun_return_mut[call_target];
    } else {
    }
    int indirect_ret_arg = tc_nth_arg(node_b[id], indirect_ret_param);
    int indirect_ret_source = tc_provenance(indirect_ret_arg);
    int indirect_ret_life = tc_provenance_param(indirect_ret_arg);
    (void)(tc_type_node(node_b[fty]));
    if (indirect_ret_param > 0) {
      tc_expr_borrow_source = indirect_ret_source;
      tc_expr_borrow_mut = indirect_ret_mut;
      tc_expr_borrow_param = indirect_ret_life;
    } else {
    }
    return;
  } else {
  }
  if (k == N_BINOP) {
    (void)(tc_expr(node_a[id]));
    int ak = tc_kind;
    int an = tc_name;
    int ae = tc_elem_kind;
    int aen = tc_elem_name;
    (void)(tc_expr(node_b[id]));
    int bk = tc_kind;
    int bn = tc_name;
    int be = tc_elem_kind;
    int ben = tc_elem_name;
    if (node_value[id] == OP_CONCAT) {
      if ((ak != TY_STRING) || (bk != TY_STRING))
        (void)(tc_fail(14));
      else {
      }
      tc_kind = TY_STRING;
      tc_name = 0;
    } else if ((node_value[id] == OP_AND) || (node_value[id] == OP_OR)) {
      if ((tc_is_integer_kind(ak) == 0) || (tc_is_integer_kind(bk) == 0))
        (void)(tc_fail(15));
      else {
      }
      tc_kind = TY_BOOL;
      tc_name = 0;
    } else if (((((node_value[id] == OP_BITAND) || (node_value[id] == OP_BITOR)) ||
                 (node_value[id] == OP_BITXOR)) ||
                (node_value[id] == OP_SHL)) ||
               (node_value[id] == OP_SHR)) {
      if ((tc_is_integer_kind(ak) == 0) || (tc_is_integer_kind(bk) == 0))
        (void)(tc_fail(32));
      else {
      }
      tc_kind = tc_integer_result_kind(ak, bk);
      tc_name = 0;
    } else if ((node_value[id] == OP_EQ) || (node_value[id] == OP_NEQ)) {
      int null_cmp = 0;
      if (((((ak == TY_PTR) || (ak == TY_FUN)) && (bk == TY_INT)) &&
           (node_kind[node_b[id]] == N_INT)) &&
          (node_value[node_b[id]] == 0))
        null_cmp = 1;
      else {
      }
      if (((((bk == TY_PTR) || (bk == TY_FUN)) && (ak == TY_INT)) &&
           (node_kind[node_a[id]] == N_INT)) &&
          (node_value[node_a[id]] == 0))
        null_cmp = 1;
      else {
      }
      if ((((tc_same_full(ak, an, ae, aen, bk, bn, be, ben) == 0) && (null_cmp == 0)) &&
           (tc_integer_literal_conversion(node_a[id], bk, ak) == 0)) &&
          (tc_integer_literal_conversion(node_b[id], ak, bk) == 0))
        (void)(tc_fail(16));
      else {
      }
      tc_kind = TY_BOOL;
      tc_name = 0;
    } else if ((((node_value[id] == OP_LT) || (node_value[id] == OP_GT)) ||
                (node_value[id] == OP_LE)) ||
               (node_value[id] == OP_GE)) {
      if ((tc_is_numeric_kind(ak) == 0) || (tc_is_numeric_kind(bk) == 0))
        (void)(tc_fail(17));
      else {
      }
      tc_kind = TY_BOOL;
      tc_name = 0;
    } else if (node_value[id] == OP_ADD) {
      if (((ak == TY_PTR) && (tc_is_integer_kind(bk) == 1)) && (ae != TY_VOID)) {
        tc_kind = TY_PTR;
        tc_name = an;
        tc_elem_kind = ae;
        tc_elem_name = aen;
      } else if (((tc_is_integer_kind(ak) == 1) && (bk == TY_PTR)) && (be != TY_VOID)) {
        tc_kind = TY_PTR;
        tc_name = bn;
        tc_elem_kind = be;
        tc_elem_name = ben;
      } else if ((tc_is_numeric_kind(ak) == 1) && (tc_is_numeric_kind(bk) == 1)) {
        tc_kind = tc_numeric_result_kind(ak, bk);
        tc_name = 0;
      } else
        (void)(tc_fail(18));
    } else if (node_value[id] == OP_SUB) {
      if (((ak == TY_PTR) && (tc_is_integer_kind(bk) == 1)) && (ae != TY_VOID)) {
        tc_kind = TY_PTR;
        tc_name = an;
        tc_elem_kind = ae;
        tc_elem_name = aen;
      } else if (tc_ptr_diff_ok(ak, ae, aen, bk, be, ben) == 1) {
        tc_kind = TY_INT;
        tc_name = 0;
        tc_elem_kind = 0;
        tc_elem_name = 0;
      } else if ((tc_is_numeric_kind(ak) == 1) && (tc_is_numeric_kind(bk) == 1)) {
        tc_kind = tc_numeric_result_kind(ak, bk);
        tc_name = 0;
      } else
        (void)(tc_fail(18));
    } else if (((node_value[id] == OP_MUL) || (node_value[id] == OP_DIV)) ||
               (node_value[id] == OP_MOD)) {
      if ((tc_is_numeric_kind(ak) == 0) || (tc_is_numeric_kind(bk) == 0))
        (void)(tc_fail(18));
      else {
      }
      tc_kind = tc_numeric_result_kind(ak, bk);
      tc_name = 0;
    } else {
      (void)(tc_fail(18));
    }
    tc_result_type = tc_type_node_from_summary(tc_kind, tc_name, tc_elem_kind, tc_elem_name);
    node_aux[id] = tc_result_type;
    return;
  } else {
  }
  (void)(tc_fail(19));
}
int tc_find_function(int name) {
  int item = node_a[tc_root];
  while (item != 0) {
    if ((((node_kind[item] == N_FUNC) || (node_kind[item] == N_GENERIC_FUNC)) ||
         (node_kind[item] == N_EXTERN)) &&
        (node_value[item] == name))
      return item;
    else {
    }
    item = node_next[item];
  }
  return 0;
}
int sym_find_last_scope(int name) {
  int i = (sym_len[name] - 1);
  while (i > 1) {
    if ((source[(sym_start[name] + i)] == 58) && (source[((sym_start[name] + i) - 1)] == 58))
      return (i - 1);
    else {
    }
    i = (i - 1);
  }
  return 0;
}
int sym_prefix_scope(int name, int length) {
  if (length <= 0)
    return 0;
  else {
  }
  int start = (source_len + sym_text_len);
  int i = 0;
  while (i < length) {
    (void)(ensure_source((start + i)));
    source[(start + i)] = source[(sym_start[name] + i)];
    i = (i + 1);
  }
  int id = sym_intern(start, length, L_ID, 0);
  sym_text_len = (sym_text_len + length);
  return id;
}
int tc_find_function_ctx(int name, int ns) {
  int exact = tc_find_function(name);
  if (exact != 0)
    return exact;
  else {
  }
  if (ns == 0)
    return 0;
  else {
  }
  int scope = ns;
  while (scope != 0) {
    int candidate = sym_qualified(scope, name);
    int item = tc_find_function(candidate);
    if (item != 0)
      return item;
    else {
    }
    int split = sym_find_last_scope(scope);
    if (split == 0)
      scope = 0;
    else
      scope = sym_prefix_scope(scope, split);
  }
  return 0;
}
int tc_find_enum_value(int name) {
  int item = node_a[tc_root];
  while (item != 0) {
    if (node_kind[item] == N_ENUM) {
      int f = node_a[item];
      while (f != 0) {
        if (node_a[f] == name)
          return node_value[item];
        else {
        }
        f = node_next[f];
      }
    } else {
    }
    item = node_next[item];
  }
  return 0;
}
int tc_find_enum_variant(int name) {
  tc_variant_enum = 0;
  tc_variant_member = 0;
  int item = node_a[tc_root];
  while (item != 0) {
    if (node_kind[item] == N_ENUM) {
      int f = node_a[item];
      while (f != 0) {
        int qualified = sym_qualified(node_value[item], node_a[f]);
        if (qualified == name) {
          tc_variant_enum = node_value[item];
          tc_variant_member = f;
          return 1;
        } else {
        }
        f = node_next[f];
      }
    } else {
    }
    item = node_next[item];
  }
  return 0;
}
int tc_match_enum_decl(int ty) {
  if ((ty == 0) || (node_kind[ty] != TY_NAMED))
    return 0;
  else {
  }
  int decl = tc_find_enum_ctx(node_value[ty], node_scope[ty]);
  if (decl == 0)
    decl = tc_find_enum(node_value[ty]);
  else {
  }
  return decl;
}
int tc_match_variant_member(int decl, int name) {
  if ((decl == 0) || (node_kind[decl] != N_ENUM))
    return 0;
  else {
  }
  int field = node_a[decl];
  while (field != 0) {
    if (node_a[field] == name)
      return field;
    else {
    }
    int qualified = sym_qualified(node_value[decl], node_a[field]);
    if (qualified == name)
      return field;
    else {
    }
    field = node_next[field];
  }
  return 0;
}
int tc_match_seen_variant(int head, int member) {
  int arm = head;
  while (arm != 0) {
    if (node_aux[arm] == member)
      return 1;
    else {
    }
    arm = node_next[arm];
  }
  return 0;
}
void tc_match_check_arm_bindings(int variant, int bindings) {
  int field = node_b[variant];
  int binding = bindings;
  while ((field != 0) && (binding != 0)) {
    (void)(tc_type_node(node_b[field]));
    int bk = tc_kind;
    int bn = tc_name;
    int bek = tc_elem_kind;
    int ben = tc_elem_name;
    (void)(tc_add_var(node_value[binding], bk, bn, bek, ben, node_b[field]));
    binding = node_next[binding];
    field = node_next[field];
  }
  if ((field != 0) || (binding != 0))
    (void)(tc_fail(50));
  else {
  }
}
int tc_emit_field_type(int id) {
  if ((id == 0) || (node_kind[id] != N_FIELD_ACCESS))
    return 0;
  else {
  }
  int base_ty = tc_emit_arg_type(node_a[id]);
  if (base_ty == 0)
    return 0;
  else {
  }
  base_ty = gen_substitute_type(base_ty);
  if (node_kind[base_ty] == TY_VARIANT) {
    int variant_item = node_value[base_ty];
    int variant_field = node_b[variant_item];
    while (variant_field != 0) {
      if (node_a[variant_field] == node_value[id])
        return gen_substitute_type(node_b[variant_field]);
      else {
      }
      variant_field = node_next[variant_field];
    }
    return 0;
  } else {
  }
  int struct_name = 0;
  int args = 0;
  if (node_kind[base_ty] == TY_GENERIC) {
    struct_name = node_value[base_ty];
    args = node_a[base_ty];
  } else if (node_kind[base_ty] == TY_NAMED)
    struct_name = node_value[base_ty];
  else
    return 0;
  int decl = tc_find_struct(struct_name);
  if (decl == 0)
    return 0;
  else {
  }
  int field_bind_pushed = 0;
  if (node_kind[decl] == N_GENERIC_STRUCT) {
    int gp = node_c[decl];
    int ga = args;
    (void)(tc_bind_push());
    field_bind_pushed = 1;
    (void)(tc_bind_clear());
    while ((gp != 0) && (ga != 0)) {
      if (tc_bind_add(node_a[gp], ga) == 0) {
        (void)(tc_bind_pop());
        return 0;
      } else {
      }
      gp = node_next[gp];
      ga = node_next[ga];
    }
  } else {
  }
  int f = node_a[decl];
  while (f != 0) {
    if (node_a[f] == node_value[id]) {
      int result_field_type = tc_substitute_type(node_b[f]);
      if (field_bind_pushed == 1)
        (void)(tc_bind_pop());
      else {
      }
      return result_field_type;
    } else {
    }
    f = node_next[f];
  }
  if (field_bind_pushed == 1)
    (void)(tc_bind_pop());
  else {
  }
  return 0;
}
int tc_emit_arg_type(int id) {
  if (id == 0)
    return 0;
  else {
  }
  if ((node_type[id] != 0) &&
      (((node_kind[id] == N_VAR) || (node_kind[id] == N_DEREF)) || (node_kind[id] == N_INDEX)))
    return gen_substitute_type(node_type[id]);
  else {
  }
  if (node_kind[id] == N_ADDRESS) {
    if (node_kind[node_a[id]] == N_VAR) {
      int addressed_function = tc_find_function_ctx(node_value[node_a[id]], node_scope[node_a[id]]);
      if (addressed_function != 0)
        return tc_signature_type(addressed_function);
      else {
      }
    } else {
    }
    (void)(tc_expr(id));
    return tc_result_type;
  } else {
  }
  if ((node_kind[id] == N_VARIANT) && (node_aux[id] != 0))
    return gen_substitute_type(node_aux[id]);
  else {
  }
  if (node_kind[id] == N_CLOSURE) {
    if (node_aux[id] > 0) {
      int closure_serial_value = (node_aux[id] - 1);
      int closure_scan = 0;
      while (closure_scan < gen_closure_count) {
        if (gen_closure_serial[closure_scan] == closure_serial_value)
          return gen_closure_sig[closure_scan];
        else {
        }
        closure_scan = (closure_scan + 1);
      }
    } else {
    }
    return gen_closure_signature(id);
  } else {
  }
  if (((node_kind[id] == N_FIELD_ACCESS) && (node_aux[id] != 0)) &&
      (node_kind[node_aux[id]] == TY_VARIANT))
    return node_aux[id];
  else {
  }
  if (node_kind[id] == N_FIELD_ACCESS) {
    int field_ty = tc_emit_field_type(id);
    if (field_ty != 0)
      return field_ty;
    else {
    }
  } else {
  }
  if (node_kind[id] == N_INDIRECT_CALL) {
    int callee_type = tc_emit_arg_type(node_a[id]);
    if ((callee_type != 0) &&
        ((node_kind[callee_type] == TY_FUN) || (node_kind[callee_type] == TY_CLOSURE)))
      return gen_substitute_type(node_b[callee_type]);
    else {
    }
  } else {
  }
  if ((node_kind[id] == N_CALL) && (node_aux[id] != 0))
    return gen_substitute_type(node_aux[id]);
  else {
  }
  if (node_kind[id] == N_CALL) {
    int call_param_type = gen_active_param_type(node_value[id]);
    if ((call_param_type != 0) &&
        ((node_kind[call_param_type] == TY_FUN) || (node_kind[call_param_type] == TY_CLOSURE)))
      return gen_substitute_type(node_b[call_param_type]);
    else {
    }
  } else {
  }
  if (node_kind[id] == N_VAR) {
    int stable_type = node_type[id];
    if (stable_type != 0)
      return gen_substitute_type(stable_type);
    else {
    }
    int formal_type = gen_active_param_type(node_value[id]);
    if (formal_type != 0)
      return gen_substitute_type(formal_type);
    else {
    }
    int declared_type = gen_active_local_decl_type(node_value[id]);
    if (declared_type != 0)
      return gen_substitute_type(declared_type);
    else {
    }
    if (node_aux[id] != 0) {
      int local_type = node_aux[id];
      int resolved_local_type = gen_substitute_type(local_type);
      if (resolved_local_type != 0)
        return resolved_local_type;
      else {
      }
      return local_type;
    } else {
    }
  } else {
  }
  if (node_kind[id] == N_STRING)
    return ast_node(TY_STRING, 0, 0, 0, 0, 0);
  else {
  }
  if (node_kind[id] == N_INT)
    return ast_node(TY_INT, 0, 0, 0, 0, 0);
  else {
  }
  if (node_kind[id] == N_BOOL)
    return ast_node(TY_BOOL, 0, 0, 0, 0, 0);
  else {
  }
  if (node_kind[id] == N_CHAR)
    return ast_node(TY_CHAR, 0, 0, 0, 0, 0);
  else {
  }
  if (node_kind[id] == N_FLOAT) {
    if (node_aux[id] == TY_FLOAT)
      return ast_node(TY_FLOAT, 0, 0, 0, 0, 0);
    else {
    }
    return ast_node(TY_DOUBLE, 0, 0, 0, 0, 0);
  } else {
  }
  if (node_kind[id] == N_NULL)
    return ast_node(TY_PTR, ast_node(TY_VOID, 0, 0, 0, 0, 0), 0, 0, 0, 0);
  else {
  }
  if (node_kind[id] == N_TUPLE) {
    if (node_aux[id] != 0)
      return gen_substitute_type(node_aux[id]);
    else {
    }
    (void)(tc_expr(id));
    return tc_result_type;
  } else {
  }
  int ek = gen_expr_kind(id);
  return tc_type_node_from_summary(ek, 0, tc_elem_kind, tc_elem_name);
}
int tc_expr_kind_for_emit(int id) {
  if ((id != 0) && (node_kind[id] == N_CLOSURE))
    return TY_CLOSURE;
  else {
  }
  if (((id != 0) && (node_kind[id] == N_CALL)) && (node_aux[id] != 0))
    return node_kind[node_aux[id]];
  else {
  }
  if ((id != 0) && (node_kind[id] == N_INDIRECT_CALL)) {
    int indirect_result_type = tc_emit_arg_type(id);
    if (indirect_result_type != 0)
      return node_kind[indirect_result_type];
    else {
    }
    return TY_INT;
  } else {
  }
  if ((id != 0) && (node_kind[id] == N_CALL)) {
    int call_param_type = gen_active_param_type(node_value[id]);
    if ((call_param_type != 0) &&
        ((node_kind[call_param_type] == TY_FUN) || (node_kind[call_param_type] == TY_CLOSURE))) {
      int call_result_type = gen_substitute_type(node_b[call_param_type]);
      if (call_result_type != 0)
        return node_kind[call_result_type];
      else {
      }
    } else {
    }
  } else {
  }
  int f = tc_find_function_ctx(node_value[id], node_scope[id]);
  if (f != 0) {
    if (node_kind[f] == N_GENERIC_FUNC) {
      int actual = node_b[id];
      int a = node_a[id];
      if (actual == 0) {
        while (a != 0) {
          int q = tc_emit_arg_type(a);
          if (q != 0) {
            if (actual == 0)
              actual = q;
            else
              actual = ast_link(actual, q);
          } else {
          }
          a = node_next[a];
        }
      } else {
      }
      int saved_count = gen_bind_count;
      (void)(ensure_gen_bind((saved_count + saved_count)));
      int save_i = 0;
      while (save_i < saved_count) {
        gen_bind_name[(saved_count + save_i)] = gen_bind_name[save_i];
        gen_bind_type[(saved_count + save_i)] = gen_bind_type[save_i];
        save_i = (save_i + 1);
      }
      (void)(gen_bind_decl(f, actual));
      int ret = gen_substitute_type(node_b[f]);
      int result_kind = TY_INT;
      if (ret != 0) {
        result_kind = node_kind[ret];
      } else {
      }
      (void)(gen_bind_clear());
      int restore_i = 0;
      while (restore_i < saved_count) {
        gen_bind_name[restore_i] = gen_bind_name[(saved_count + restore_i)];
        gen_bind_type[restore_i] = gen_bind_type[(saved_count + restore_i)];
        restore_i = (restore_i + 1);
      }
      gen_bind_count = saved_count;
      if (result_kind == TY_PARAM) {
        return TY_INT;
      } else {
      }
      return result_kind;
    } else {
    }
    if (node_b[f] != 0) {
      return node_kind[node_b[f]];
    } else {
    }
  } else {
  }
  return TY_INT;
}
void tc_stmt(int id, int expected_kind, int expected_name) {
  if ((id != 0) && (tc_ok == 1))
    tc_error_pos = node_pos[id];
  else {
  }
  if ((tc_ok == 0) || (id == 0))
    return;
  else {
  }
  int k = node_kind[id];
  if (k == N_CONST) {
    (void)(tc_type_node(node_b[id]));
    int ck = tc_kind;
    int cn = tc_name;
    int ce = tc_elem_kind;
    int cen = tc_elem_name;
    (void)(tc_mark_float_expr(node_c[id], ck));
    (void)(tc_expr(node_c[id]));
    int const_ffi_borrowed = tc_expr_ffi_borrowed;
    int const_fun_target = tc_expr_fun_target;
    if (tc_literal_fits(node_c[id], ck) == 0)
      (void)(tc_fail(54));
    else {
    }
    if (tc_same_full(ck, cn, ce, cen, tc_kind, tc_name, tc_elem_kind, tc_elem_name) == 0) {
      if (((tc_integer_literal_conversion(node_c[id], ck, tc_kind) == 0) &&
           ((node_kind[node_c[id]] != N_INT) || (node_value[node_c[id]] != 0))) &&
          (node_kind[node_c[id]] != N_NULL))
        (void)(tc_fail(30));
      else {
      }
    } else {
    }
    (void)(tc_add_var(node_a[id], ck, cn, ce, cen, node_b[id]));
    sym_type[node_a[id]] = (ck + 100);
    tc_var_const[tc_last_var_index] = 1;
    tc_var_fun_target[tc_last_var_index] = const_fun_target;
    tc_var_ffi_borrowed[tc_last_var_index] = const_ffi_borrowed;
  } else if (k == N_LET) {
    (void)(tc_type_node(node_b[id]));
    int dk = tc_kind;
    int dn = tc_name;
    int de = tc_elem_kind;
    int den = tc_elem_name;
    (void)(tc_mark_float_expr(node_c[id], dk));
    (void)(tc_expr(node_c[id]));
    int ek = tc_kind;
    int en = tc_name;
    int ee = tc_elem_kind;
    int een = tc_elem_name;
    int rhs_borrow = tc_expr_borrow_source;
    int rhs_borrow_origin = tc_expr_borrow_origin;
    int rhs_borrow_mut = tc_expr_borrow_mut;
    int rhs_borrow_param = tc_expr_borrow_param;
    int rhs_ffi_borrowed = tc_expr_ffi_borrowed;
    int rhs_fun_target = tc_expr_fun_target;
    if ((tc_is_owner_kind(dk) == 1) && (node_kind[node_c[id]] == N_VAR))
      (void)(tc_fail(40));
    else {
    }
    if (tc_literal_fits(node_c[id], dk) == 0)
      (void)(tc_fail(54));
    else {
    }
    if (tc_same_full(dk, dn, de, den, ek, en, ee, een) == 0) {
      if (((tc_integer_literal_conversion(node_c[id], dk, ek) == 0) &&
           ((node_kind[node_c[id]] != N_INT) || (node_value[node_c[id]] != 0))) &&
          (node_kind[node_c[id]] != N_NULL))
        (void)(tc_fail_types(20, dk, ek));
      else {
      }
    } else {
    }
    if ((dk == TY_DYN_ARRAY) && (node_kind[node_c[id]] == N_VAR))
      (void)(tc_move_value(node_c[id]));
    else {
    }
    (void)(tc_add_var(node_a[id], dk, dn, de, den, node_b[id]));
    tc_var_fun_target[tc_last_var_index] = rhs_fun_target;
    tc_var_ffi_borrowed[tc_last_var_index] = rhs_ffi_borrowed;
    if ((tc_is_owner_kind(dk) == 1) || (tc_owned_initializer(node_c[id]) == 1))
      tc_var_owned[tc_last_var_index] = 1;
    else {
    }
    if ((dk == TY_CLOSURE) && (node_kind[node_c[id]] == N_CLOSURE))
      (void)(tc_attach_closure_caps(tc_last_var_index, node_a[node_c[id]]));
    else {
    }
    if (dk == TY_PTR) {
      if (rhs_borrow < 0) {
      } else {
        if (rhs_borrow_mut == 1)
          (void)(tc_record_borrow_ex(tc_last_var_index, rhs_borrow, 3, rhs_borrow_origin));
        else
          (void)(tc_record_borrow_ex(tc_last_var_index, rhs_borrow, 2, rhs_borrow_origin));
        tc_var_borrow_param[tc_last_var_index] = rhs_borrow_param;
      }
    } else {
    }
  } else if ((k == N_ASSIGN) || (k == N_COMPOUND_ASSIGN)) {
    if (tc_place_is_const(node_a[id]) == 1)
      (void)(tc_fail(31));
    else {
    }
    int lhs_field_root = (0 - 1);
    int lhs_field_name = 0;
    if (node_kind[node_a[id]] == N_FIELD_ACCESS) {
      lhs_field_root = tc_field_target_root_for_place(node_a[node_a[id]]);
      lhs_field_name = node_value[node_a[id]];
    } else {
    }
    (void)(tc_expr(node_a[id]));
    int lk = tc_kind;
    int ln = tc_name;
    int le = tc_elem_kind;
    int len = tc_elem_name;
    int lhs_index = tc_last_var_index;
    (void)(tc_require_mutable(node_a[id]));
    (void)(tc_mark_float_expr(node_b[id], lk));
    (void)(tc_expr(node_b[id]));
    int rhs_borrow_assign = tc_expr_borrow_source;
    int rhs_borrow_origin_assign = tc_expr_borrow_origin;
    int rhs_borrow_mut_assign = tc_expr_borrow_mut;
    int rhs_borrow_param_assign = tc_expr_borrow_param;
    int rhs_ffi_borrowed_assign = tc_expr_ffi_borrowed;
    int rhs_fun_target_assign = tc_expr_fun_target;
    if (k == N_COMPOUND_ASSIGN) {
      int combined = ast_node(N_BINOP, node_a[id], node_b[id], 0, node_value[id], 0);
      (void)(tc_expr(combined));
    } else {
    }
    if ((lk == TY_DYN_ARRAY) && (node_kind[node_b[id]] == N_VAR))
      (void)(tc_fail(40));
    else {
    }
    if (tc_literal_fits(node_b[id], lk) == 0)
      (void)(tc_fail(54));
    else {
    }
    if (tc_same_full(lk, ln, le, len, tc_kind, tc_name, tc_elem_kind, tc_elem_name) == 0) {
      if (((tc_integer_literal_conversion(node_b[id], lk, tc_kind) == 0) &&
           ((node_kind[node_b[id]] != N_INT) || (node_value[node_b[id]] != 0))) &&
          (node_kind[node_b[id]] != N_NULL))
        (void)(tc_fail_types(21, lk, tc_kind));
      else {
      }
    } else {
    }
    if (((tc_ok == 1) && (node_kind[node_a[id]] == N_VAR)) && (lk == TY_DYN_ARRAY)) {
      tc_var_owned[lhs_index] = 1;
      tc_var_moved[lhs_index] = 0;
    } else {
    }
    if ((tc_ok == 1) && (lk == TY_FUN)) {
      if (node_kind[node_a[id]] == N_VAR)
        tc_var_fun_target[lhs_index] = rhs_fun_target_assign;
      else if (node_kind[node_a[id]] == N_FIELD_ACCESS) {
        (void)(tc_field_target_set(lhs_field_root, lhs_field_name, rhs_fun_target_assign));
      } else {
      }
    } else {
    }
    if (((tc_ok == 1) && (node_kind[node_a[id]] == N_VAR)) && (lk == TY_PTR)) {
      (void)(tc_release_borrow(lhs_index));
      tc_var_borrow_source[lhs_index] = (0 - 1);
      tc_var_borrow_mode[lhs_index] = 0;
      tc_var_borrow_parent[lhs_index] = (0 - 1);
      tc_var_borrow_param[lhs_index] = 0;
      tc_var_fun_target[lhs_index] = rhs_fun_target_assign;
      tc_var_ffi_borrowed[lhs_index] = rhs_ffi_borrowed_assign;
      if (rhs_borrow_assign < 0) {
      } else {
        if (rhs_borrow_mut_assign == 1)
          (void)(tc_record_borrow_ex(lhs_index, rhs_borrow_assign, 3, rhs_borrow_origin_assign));
        else
          (void)(tc_record_borrow_ex(lhs_index, rhs_borrow_assign, 2, rhs_borrow_origin_assign));
        tc_var_borrow_param[lhs_index] = rhs_borrow_param_assign;
      }
    } else {
    }
  } else if (k == N_DEFER) {
    (void)(tc_expr(node_a[id]));
    if (tc_kind != TY_VOID)
      (void)(tc_fail(46));
    else {
    }
  } else if (k == N_TUPLE_BIND) {
    (void)(tc_type_node(node_b[id]));
    int declared_ty = tc_result_type;
    int declared_kind = tc_kind;
    (void)(tc_expr(node_c[id]));
    int rhs_ty = tc_result_type;
    if ((declared_kind != TY_TUPLE) || (tc_kind != TY_TUPLE))
      (void)(tc_fail(52));
    else if (((declared_ty == 0) || (rhs_ty == 0)) || (tc_type_equal(declared_ty, rhs_ty) == 0))
      (void)(tc_fail_types(20, declared_kind, tc_kind));
    else {
      int elem = node_a[declared_ty];
      int binding = node_a[id];
      while ((elem != 0) && (binding != 0)) {
        (void)(tc_type_node(elem));
        int ek = tc_kind;
        int en = tc_name;
        int eek = tc_elem_kind;
        int een = tc_elem_name;
        (void)(tc_add_var(node_value[binding], ek, en, eek, een, elem));
        elem = node_next[elem];
        binding = node_next[binding];
      }
      if ((elem != 0) || (binding != 0))
        (void)(tc_fail(53));
      else {
      }
    }
  } else if (k == N_MATCH) {
    (void)(tc_expr(node_a[id]));
    int subject_kind = tc_kind;
    int subject_type = tc_result_type;
    int enum_decl = 0;
    if ((subject_kind == TY_NAMED) && (subject_type != 0))
      enum_decl = tc_match_enum_decl(subject_type);
    else {
    }
    if (enum_decl == 0)
      (void)(tc_fail(47));
    else {
      int arm = node_b[id];
      int has_default = 0;
      int flow_has_arm = 0;
      (void)(tc_flow_save_base());
      while (arm != 0) {
        if (node_value[arm] == 0) {
          if ((has_default == 1) || (node_next[arm] != 0))
            (void)(tc_fail(74));
          else {
          }
          has_default = 1;
          (void)(tc_enter_scope());
          (void)(tc_stmt(node_b[arm], expected_kind, expected_name));
          (void)(tc_leave_scope());
          if (flow_has_arm == 0) {
            (void)(tc_flow_save_yes());
            flow_has_arm = 1;
            (void)(tc_flow_restore_base());
          } else {
            (void)(tc_flow_merge_yes());
            (void)(tc_flow_save_yes());
            (void)(tc_flow_restore_base());
          }
        } else {
          int member = tc_match_variant_member(enum_decl, node_value[arm]);
          if (member == 0)
            (void)(tc_fail(48));
          else {
            if (tc_match_seen_variant(node_b[id], member) == 1)
              (void)(tc_fail(49));
            else {
            }
            node_aux[arm] = member;
            (void)(tc_enter_scope());
            (void)(tc_match_check_arm_bindings(member, node_a[arm]));
            (void)(tc_stmt(node_b[arm], expected_kind, expected_name));
            (void)(tc_leave_scope());
            if (flow_has_arm == 0) {
              (void)(tc_flow_save_yes());
              flow_has_arm = 1;
              (void)(tc_flow_restore_base());
            } else {
              (void)(tc_flow_merge_yes());
              (void)(tc_flow_save_yes());
              (void)(tc_flow_restore_base());
            }
          }
        }
        arm = node_next[arm];
      }
      if (has_default == 0) {
        int variant = node_a[enum_decl];
        while (variant != 0) {
          if (tc_match_seen_variant(node_b[id], variant) == 0)
            (void)(tc_fail(51));
          else {
          }
          variant = node_next[variant];
        }
      } else {
      }
      if (flow_has_arm == 1) {
        (void)(tc_flow_restore_base());
        (void)(tc_flow_merge_yes());
      } else {
      }
      (void)(tc_flow_end());
    }
  } else if ((k == N_PRINT) || (k == N_PRINTLN))
    (void)(tc_expr(node_a[id]));
  else if (k == N_EXPR) {
    (void)(tc_expr(node_a[id]));
    if (node_kind[node_a[id]] == N_CALL)
      (void)(tc_consume_call(node_a[id]));
    else {
    }
  } else if (k == N_RETURN) {
    if (node_a[id] == 0) {
      if (expected_kind != TY_VOID)
        (void)(tc_fail(22));
      else {
      }
    } else {
      (void)(tc_mark_float_expr(node_a[id], expected_kind));
      (void)(tc_expr(node_a[id]));
      int return_borrow = tc_expr_borrow_source;
      if (tc_literal_fits(node_a[id], expected_kind) == 0)
        (void)(tc_fail(54));
      else {
      }
      if (expected_kind == TY_PTR) {
        if (node_kind[node_a[id]] == N_ADDRESS)
          (void)(tc_check_explicit_return_address_escape(return_borrow));
        else
          (void)(tc_check_return_escape(return_borrow));
      } else {
      }
      if (expected_kind == TY_CLOSURE)
        (void)(tc_check_closure_value_escape(node_a[id]));
      else {
      }
      if (tc_same_full(expected_kind, expected_name, tc_expected_elem_kind, tc_expected_elem_name,
                       tc_kind, tc_name, tc_elem_kind, tc_elem_name) == 0)
        if ((tc_integer_literal_conversion(node_a[id], expected_kind, tc_kind) == 0) &&
            (node_kind[node_a[id]] != N_NULL))
          (void)(tc_fail_types(23, expected_kind, tc_kind));
        else {
        }
      else {
      }
    }
  } else if ((k == N_BREAK) || (k == N_CONTINUE)) {
    if (tc_loop_depth == 0)
      (void)(tc_fail(24));
    else {
    }
  } else if (k == N_BLOCK) {
    (void)(tc_enter_scope());
    int x = node_a[id];
    while (x != 0) {
      (void)(tc_stmt(x, expected_kind, expected_name));
      x = node_next[x];
    }
    (void)(tc_leave_scope());
  } else if (k == N_IF) {
    (void)(tc_expr(node_a[id]));
    if (((tc_is_numeric_kind(tc_kind) == 0) && (tc_kind != TY_PTR)) && (tc_kind != TY_FUN))
      (void)(tc_fail(25));
    else {
    }
    if ((node_kind[node_b[id]] == N_DEFER) || (node_kind[node_c[id]] == N_DEFER))
      (void)(tc_fail(77));
    else {
    }
    if ((((node_kind[node_b[id]] == N_LET) || (node_kind[node_c[id]] == N_LET)) ||
         (node_kind[node_b[id]] == N_TUPLE_BIND)) ||
        (node_kind[node_c[id]] == N_TUPLE_BIND))
      (void)(tc_fail(78));
    else {
    }
    (void)(tc_flow_save_base());
    (void)(tc_stmt(node_b[id], expected_kind, expected_name));
    (void)(tc_flow_save_yes());
    (void)(tc_flow_restore_base());
    (void)(tc_stmt(node_c[id], expected_kind, expected_name));
    (void)(tc_flow_merge_yes());
    (void)(tc_flow_end());
  } else if (k == N_WHILE) {
    (void)(tc_expr(node_a[id]));
    if (((tc_is_numeric_kind(tc_kind) == 0) && (tc_kind != TY_PTR)) && (tc_kind != TY_FUN))
      (void)(tc_fail(26));
    else {
    }
    (void)(tc_flow_save_base());
    tc_loop_depth = (tc_loop_depth + 1);
    (void)(tc_stmt(node_b[id], expected_kind, expected_name));
    tc_loop_depth = (tc_loop_depth - 1);
    if (node_value[id] != 0)
      (void)(tc_stmt(node_value[id], expected_kind, expected_name));
    else {
    }
    (void)(tc_flow_save_yes());
    (void)(tc_flow_restore_base());
    (void)(tc_flow_merge_yes());
    (void)(tc_flow_end());
  } else if (k == N_FOR) {
    (void)(tc_enter_scope());
    if (node_a[id] != 0)
      (void)(tc_stmt(node_a[id], expected_kind, expected_name));
    else {
    }
    (void)(tc_expr(node_b[id]));
    if (((tc_is_numeric_kind(tc_kind) == 0) && (tc_kind != TY_PTR)) && (tc_kind != TY_FUN))
      (void)(tc_fail(27));
    else {
    }
    (void)(tc_flow_save_base());
    tc_loop_depth = (tc_loop_depth + 1);
    (void)(tc_stmt(node_c[id], expected_kind, expected_name));
    tc_loop_depth = (tc_loop_depth - 1);
    if (node_value[id] != 0)
      (void)(tc_stmt(node_value[id], expected_kind, expected_name));
    else {
    }
    (void)(tc_flow_save_yes());
    (void)(tc_flow_restore_base());
    (void)(tc_flow_merge_yes());
    (void)(tc_flow_end());
    (void)(tc_leave_scope());
  } else {
  }
}
int tc_diag_line(int pos) {
  int i = 0;
  int line = 1;
  while ((i < pos) && (i < source_len)) {
    if (source[i] == 10)
      line = (line + 1);
    else {
    }
    i = (i + 1);
  }
  return line;
}
int tc_diag_col(int pos) {
  int i = 0;
  int col = 1;
  while ((i < pos) && (i < source_len)) {
    if (source[i] == 10)
      col = 1;
    else
      col = (col + 1);
    i = (i + 1);
  }
  return col;
}
int tc_diag_file(int pos) {
  if (pos < 0)
    return 0;
  else {
  }
  if (pos < source_len)
    return source_file_at[pos];
  else {
  }
  if (source_len > 0)
    return source_file_at[(source_len - 1)];
  else {
  }
  return source_active_file;
}
void tc_print_source_byte(int value) {
  if (value == 9)
    (void)(runtime_write_char(9));
  else if (value == 10)
    (void)(runtime_write_char(10));
  else if (value == 13)
    (void)(runtime_write_char(13));
  else if (value > 31) {
    if (value < 127)
      (void)(runtime_write_char(tc_diag_ascii[(value - 32)]));
    else
      (void)(runtime_write_char(63));
  } else
    (void)(runtime_write_char(63));
}
void tc_print_source_file(int file_id) {
  if (file_id < 1)
    (void)(runtime_write_string("<unknown>"));
  else if (file_id > (source_file_count - 1))
    (void)(runtime_write_string("<unknown>"));
  else {
    int i = 0;
    while (i < source_file_name_len[file_id]) {
      (void)(tc_print_source_byte(source_file_name_text[(source_file_name_start[file_id] + i)]));
      i = (i + 1);
    }
  }
}
void tc_print_source_excerpt(int pos) {
  int begin = pos;
  int end = pos;
  if (begin < 0)
    begin = 0;
  else {
  }
  if (begin > source_len)
    begin = source_len;
  else {
  }
  while ((begin > 0) && (source[(begin - 1)] != 10)) {
    begin = (begin - 1);
  }
  while ((end < source_len) && (source[end] != 10)) {
    end = (end + 1);
  }
  int i = begin;
  while (i < end) {
    (void)(tc_print_source_byte(source[i]));
    i = (i + 1);
  }
}
void tc_print_type_kind(int kind) {
  if (kind == TY_INT)
    (void)(runtime_write_string("int"));
  else if (kind == TY_BOOL)
    (void)(runtime_write_string("bool"));
  else if (kind == TY_STRING)
    (void)(runtime_write_string("string"));
  else if (kind == TY_VOID)
    (void)(runtime_write_string("void"));
  else if (kind == TY_PTR)
    (void)(runtime_write_string("pointer"));
  else if (kind == TY_ARRAY)
    (void)(runtime_write_string("array"));
  else if (kind == TY_DYN_ARRAY)
    (void)(runtime_write_string("Array<T>"));
  else if (kind == TY_NAMED)
    (void)(runtime_write_string("named type"));
  else if (kind == TY_CHAR)
    (void)(runtime_write_string("char"));
  else if (kind == TY_FLOAT)
    (void)(runtime_write_string("f32"));
  else if (kind == TY_DOUBLE)
    (void)(runtime_write_string("f64"));
  else if (kind == TY_FUN)
    (void)(runtime_write_string("function"));
  else if (kind == TY_PARAM)
    (void)(runtime_write_string("type parameter"));
  else if (kind == TY_GENERIC)
    (void)(runtime_write_string("generic type"));
  else if (kind == TY_LONG)
    (void)(runtime_write_string("long"));
  else if (kind == TY_LLONG)
    (void)(runtime_write_string("long long"));
  else if (kind == TY_VARIANT)
    (void)(runtime_write_string("enum variant"));
  else if (kind == TY_U8)
    (void)(runtime_write_string("u8"));
  else if (kind == TY_U16)
    (void)(runtime_write_string("u16"));
  else if (kind == TY_U32)
    (void)(runtime_write_string("u32"));
  else if (kind == TY_U64)
    (void)(runtime_write_string("u64"));
  else if (kind == TY_I8)
    (void)(runtime_write_string("i8"));
  else if (kind == TY_I16)
    (void)(runtime_write_string("i16"));
  else if (kind == TY_I32)
    (void)(runtime_write_string("i32"));
  else if (kind == TY_I64)
    (void)(runtime_write_string("i64"));
  else if (kind == TY_USIZE)
    (void)(runtime_write_string("usize"));
  else if (kind == TY_TUPLE)
    (void)(runtime_write_string("tuple"));
  else
    (void)(runtime_write_string("unknown type"));
}
int tc_diag_has_types(int code) {
  if (code == 12)
    return 1;
  else {
  }
  if (code == 20)
    return 1;
  else {
  }
  if (code == 21)
    return 1;
  else {
  }
  if (code == 23)
    return 1;
  else {
  }
  if (code == 36)
    return 1;
  else {
  }
  return 0;
}
void tc_print_hint(int code) {
  if (code == 12)
    (void)(runtime_write_string("check each argument against its parameter type"));
  else if (code == 20)
    (void)(runtime_write_string("make the initializer expression match the declared type"));
  else if (code == 21)
    (void)(runtime_write_string("make the right-hand side match the left-hand side type"));
  else if (code == 23)
    (void)(runtime_write_string("return a value matching the function return type"));
  else if (code == 36)
    (void)(runtime_write_string("use an element type matching the existing array"));
  else if (code == 55)
    (void)(runtime_write_string(
        "use only scalar, pointer, fixed-array, or named struct types in extern parameters"));
  else if (code == 56)
    (void)(runtime_write_string(
        "use only scalar, pointer, fixed-array, or named struct types in extern return type"));
  else if (code == 57)
    (void)(runtime_write_string(
        "header path must contain only alphanumeric characters, '.', '/', '_', '-'"));
  else if (code == 65)
    (void)(runtime_write_string("use the default borrowed C parameter or borrow/borrow_mut with a "
                                "pointer-compatible type"));
  else if (code == 66)
    (void)(runtime_write_string("use borrow or borrow_mut only with string or pointer parameters"));
  else if (code == 67)
    (void)(runtime_write_string("do not pass an extern-returned pointer or string to memory_free; "
                                "call the matching C release API instead"));
  else if (code == 58)
    (void)(runtime_write_string("move only an owned binding or an owned container result"));
  else if (code == 59)
    (void)(runtime_write_string(
        "pass a tracked variable or address expression to a borrow parameter"));
  else if (code == 60)
    (void)(runtime_write_string(
        "keep a borrowed capture within the source binding's lexical lifetime"));
  else if (code == 61)
    (void)(runtime_write_string("closure moved-capture used after move"));
  else if (code == 31)
    (void)(runtime_write_string("assign only to a mutable binding"));
  else if (code == 68)
    (void)(runtime_write_string(
        "borrow target must be a variable, dereference, index, or field place"));
  else if (code == 69)
    (void)(runtime_write_string("return-borrow lifetime must come from one consistent parameter"));
  else if (code == 70)
    (void)(runtime_write_string("mutable borrow requires a mutable place"));
  else if (code == 74)
    (void)(runtime_write_string("default match arm must appear once and last"));
  else if (code == 75)
    (void)(runtime_write_string("do not move an owned value from a loop-carried binding"));
  else if (code == 76)
    (void)(runtime_write_string(
        "explicit generic arguments must match a generic function's parameters"));
  else if (code == 77)
    (void)(runtime_write_string("put defer inside a block-scoped statement body"));
  else if (code == 78)
    (void)(runtime_write_string(
        "put a local declaration or tuple binding inside a block-scoped statement body"));
  else if (code == 72)
    (void)(runtime_write_string("reference return escapes its owner"));
  else if (code == 73)
    (void)(runtime_write_string(
        "function-pointer target is unknown for an ownership-sensitive signature"));
  else
    (void)(runtime_write_string("inspect the expression at the reported source location"));
}
void tc_diag(void) {
  if (tc_error_code == 3)
    (void)(runtime_write_string("type error: duplicate declaration"));
  else if (tc_error_code == 5)
    (void)(runtime_write_string("type error: unknown name"));
  else if (tc_error_code == 12)
    (void)(runtime_write_string("type error: invalid function arguments"));
  else if (tc_error_code == 13)
    (void)(runtime_write_string("type error: invalid argument count"));
  else if (tc_error_code == 14)
    (void)(runtime_write_string("type error: string concatenation requires strings"));
  else if (tc_error_code == 17)
    (void)(runtime_write_string("type error: invalid built-in argument type"));
  else if (tc_error_code == 18)
    (void)(runtime_write_string("type error: invalid arithmetic operands"));
  else if (tc_error_code == 20)
    (void)(runtime_write_string("type error: initializer type mismatch"));
  else if (tc_error_code == 21)
    (void)(runtime_write_string("type error: assignment type mismatch"));
  else if (tc_error_code == 23)
    (void)(runtime_write_string("type error: return type mismatch"));
  else if (tc_error_code == 28)
    (void)(runtime_write_string("type error: recursive struct definition"));
  else if (tc_error_code == 31)
    (void)(runtime_write_string("type error: assignment to const"));
  else if (tc_error_code == 33)
    (void)(runtime_write_string("type error: use after ownership move"));
  else if (tc_error_code == 34)
    (void)(runtime_write_string("type error: use after ownership move"));
  else if (tc_error_code == 35)
    (void)(runtime_write_string("type error: release requires an owned value"));
  else if (tc_error_code == 36)
    (void)(runtime_write_string("type error: array element type mismatch"));
  else if (tc_error_code == 37)
    (void)(runtime_write_string("type error: cannot mutate or move while borrowed"));
  else if (tc_error_code == 38)
    (void)(runtime_write_string("type error: borrowed reference escapes its owner"));
  else if (tc_error_code == 40)
    (void)(runtime_write_string("type error: owned value copy requires an explicit move"));
  else if (tc_error_code == 41)
    (void)(runtime_write_string("type error: unknown function"));
  else if (tc_error_code == 42)
    (void)(runtime_write_string("type error: cannot call non-function value"));
  else if (tc_error_code == 43)
    (void)(runtime_write_string("type error: function name is reserved by the C runtime"));
  else if (tc_error_code == 45)
    (void)(runtime_write_string("type error: array index out of bounds"));
  else if (tc_error_code == 46)
    (void)(runtime_write_string("type error: defer expression must return void"));
  else if (tc_error_code == 47)
    (void)(runtime_write_string("type error: match subject must be an enum"));
  else if (tc_error_code == 48)
    (void)(runtime_write_string("type error: unknown variant in match arm"));
  else if (tc_error_code == 49)
    (void)(runtime_write_string("type error: duplicate variant in match"));
  else if (tc_error_code == 50)
    (void)(runtime_write_string("type error: match arm payload arity mismatch"));
  else if (tc_error_code == 51)
    (void)(runtime_write_string("type error: match is not exhaustive"));
  else if (tc_error_code == 52)
    (void)(runtime_write_string("type error: tuple destructuring requires tuple type"));
  else if (tc_error_code == 53)
    (void)(runtime_write_string("type error: tuple binding count mismatch"));
  else if (tc_error_code == 54)
    (void)(runtime_write_string("type error: integer literal is out of range for its target type"));
  else if (tc_error_code == 55)
    (void)(runtime_write_string("type error: extern parameter type is not FFI-safe"));
  else if (tc_error_code == 56)
    (void)(runtime_write_string("type error: extern return type is not FFI-safe"));
  else if (tc_error_code == 57)
    (void)(runtime_write_string("type error: extern header path contains invalid characters"));
  else if (tc_error_code == 65)
    (void)(runtime_write_string("type error: extern move ownership is unsupported"));
  else if (tc_error_code == 66)
    (void)(runtime_write_string(
        "type error: extern borrow mode requires a pointer-compatible type"));
  else if (tc_error_code == 67)
    (void)(runtime_write_string("type error: cannot release a borrowed extern return"));
  else if (tc_error_code == 58)
    (void)(runtime_write_string("type error: explicit move requires an owned value"));
  else if (tc_error_code == 59)
    (void)(runtime_write_string("type error: borrow parameter requires a tracked source"));
  else if (tc_error_code == 60)
    (void)(runtime_write_string("type error: closure capture escapes its source lifetime"));
  else if (tc_error_code == 61)
    (void)(runtime_write_string("type error: closure moved-capture used after move"));
  else if (tc_error_code == 68)
    (void)(runtime_write_string("type error: borrow target is not a place"));
  else if (tc_error_code == 69)
    (void)(runtime_write_string("type error: inconsistent return-borrow lifetime"));
  else if (tc_error_code == 70)
    (void)(runtime_write_string("type error: mutable borrow requires a mutable place"));
  else if (tc_error_code == 74)
    (void)(runtime_write_string("type error: default match arm must appear once and last"));
  else if (tc_error_code == 75)
    (void)(runtime_write_string("type error: owned value move is not allowed inside a loop"));
  else if (tc_error_code == 76)
    (void)(runtime_write_string(
        "type error: explicit generic arguments do not match the function parameters"));
  else if (tc_error_code == 77)
    (void)(runtime_write_string("type error: defer must be inside a block-scoped statement body"));
  else if (tc_error_code == 78)
    (void)(runtime_write_string(
        "type error: local declaration must be inside a block-scoped statement body"));
  else if (tc_error_code == 72)
    (void)(runtime_write_string("type error: reference return escapes its owner"));
  else if (tc_error_code == 73)
    (void)(runtime_write_string(
        "type error: function-pointer target is unknown for an ownership-sensitive signature"));
  else if (tc_error_code == 44)
    (void)(runtime_write_string("type error: distinct functions collide after C mangling"));
  else
    (void)(runtime_write_string("type error: invalid expression"));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.code="));
  (void)(runtime_write_int(tc_error_code));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.file="));
  (void)(tc_print_source_file(tc_diag_file(tc_error_pos)));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.line="));
  (void)(runtime_write_int(tc_diag_line(tc_error_pos)));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.column="));
  (void)(runtime_write_int(tc_diag_col(tc_error_pos)));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.hint="));
  (void)(tc_print_hint(tc_error_code));
  (void)(runtime_write_char(10));
  if (tc_diag_has_types(tc_error_code) == 1) {
    (void)(runtime_write_string("diagnostic.expected="));
    (void)(tc_print_type_kind(tc_error_expected_kind));
    (void)(runtime_write_char(10));
    (void)(runtime_write_string("diagnostic.found="));
    (void)(tc_print_type_kind(tc_error_found_kind));
    (void)(runtime_write_char(10));
  } else {
  }
  (void)(runtime_write_string("diagnostic.excerpt="));
  (void)(tc_print_source_excerpt(tc_error_pos));
  (void)(runtime_write_char(10));
}
void source_import_diag(void) {
  int code = 64;
  if (source_import_error_kind == 1)
    code = 62;
  else if (source_import_error_kind == 2)
    code = 63;
  else {
  }
  if (source_import_error_kind == 1)
    (void)(runtime_write_string("import error: dependency cycle detected"));
  else if (source_import_error_kind == 2)
    (void)(runtime_write_string("import error: module file could not be opened"));
  else
    (void)(runtime_write_string("import error: malformed or invalid include directive"));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.code="));
  (void)(runtime_write_int(code));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.file="));
  (void)(tc_print_source_file(source_import_error_file));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.line="));
  (void)(runtime_write_int(source_import_error_line));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.column=1\n"));
  (void)(runtime_write_string("diagnostic.hint="));
  if (source_import_error_kind == 1)
    (void)(runtime_write_string("remove the import edge that closes the dependency cycle"));
  else if (source_import_error_kind == 2)
    (void)(runtime_write_string("check the canonical path and ensure the module exists"));
  else
    (void)(runtime_write_string(
        "use include or includec followed by a quoted path and optional semicolon"));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.target="));
  (void)(tc_print_source_file(source_import_error_target));
  (void)(runtime_write_char(10));
  (void)(runtime_write_string("diagnostic.excerpt=include directive\n"));
}
int tc_check_function_symbols(int root) {
  int a = node_a[root];
  while (a != 0) {
    if (((node_kind[a] == N_FUNC) || (node_kind[a] == N_GENERIC_FUNC)) ||
        (node_kind[a] == N_EXTERN)) {
      int b = node_next[a];
      while (b != 0) {
        if ((((node_kind[b] == N_FUNC) || (node_kind[b] == N_GENERIC_FUNC)) ||
             (node_kind[b] == N_EXTERN)) &&
            (node_value[a] != node_value[b])) {
          int ca = sym_c_symbol(node_value[a]);
          int cb = sym_c_symbol(node_value[b]);
          if (ca == cb) {
            tc_error_code = 44;
            tc_error_pos = node_pos[b];
            tc_ok = 0;
            return 0;
          } else {
          }
        } else {
        }
        b = node_next[b];
      }
    } else {
    }
    a = node_next[a];
  }
  return 1;
}
int tc_reserved_function(int name) {
  if (bi_has_flag(name, BI_FLAG_RESERVED) == 1)
    return 1;
  else {
  }
  return 0;
}
int tc_ffi_struct_safe(int name) {
  int i = 0;
  while (i < tc_path_count) {
    if (tc_path_name[i] == name)
      return 0;
    else {
    }
    i = (i + 1);
  }
  int decl = tc_find_struct(name);
  if ((decl == 0) || (node_kind[decl] != N_STRUCT))
    return 0;
  else {
  }
  (void)(ensure_tc_path(tc_path_count));
  tc_path_name[tc_path_count] = name;
  tc_path_count = (tc_path_count + 1);
  int field = node_a[decl];
  int safe = 1;
  while (field != 0) {
    if (tc_check_ffi_type(node_b[field]) == 0)
      safe = 0;
    else {
    }
    field = node_next[field];
  }
  tc_path_count = (tc_path_count - 1);
  return safe;
}
int tc_ffi_enum_safe(int name) {
  int decl = tc_find_enum(name);
  if (decl == 0)
    return 0;
  else {
  }
  int member = node_a[decl];
  while (member != 0) {
    if (node_b[member] != 0)
      return 0;
    else {
    }
    member = node_next[member];
  }
  return 1;
}
int tc_check_ffi_type(int ty) {
  if (ty == 0)
    return 0;
  else {
  }
  int k = node_kind[ty];
  if ((((k == TY_INT) || (k == TY_BOOL)) || (k == TY_CHAR)) || (k == TY_STRING))
    return 1;
  else {
  }
  if (((k == TY_VOID) || (k == TY_FLOAT)) || (k == TY_DOUBLE))
    return 1;
  else {
  }
  if ((k == TY_LONG) || (k == TY_LLONG))
    return 1;
  else {
  }
  if (tc_is_fixed_integer_kind(k) == 1)
    return 1;
  else {
  }
  if (k == TY_PTR) {
    if (node_a[ty] == 0)
      return 0;
    else {
    }
    return tc_check_ffi_type(node_a[ty]);
  } else {
  }
  if (k == TY_ARRAY) {
    if ((node_a[ty] == 0) || (node_value[ty] < 1))
      return 0;
    else {
    }
    return tc_check_ffi_type(node_a[ty]);
  } else {
  }
  if (k == TY_NAMED) {
    int resolved_struct = tc_find_struct_ctx(node_value[ty], node_scope[ty]);
    int resolved_enum = tc_find_enum_ctx(node_value[ty], node_scope[ty]);
    if (resolved_struct != 0) {
      node_value[ty] = node_value[resolved_struct];
      return tc_ffi_struct_safe(node_value[ty]);
    } else {
    }
    if (resolved_enum != 0) {
      node_value[ty] = node_value[resolved_enum];
      return tc_ffi_enum_safe(node_value[ty]);
    } else {
    }
    return 0;
  } else {
  }
  return 0;
}
int tc_ffi_pointer_compatible(int ty) {
  if (ty == 0)
    return 0;
  else {
  }
  if ((node_kind[ty] == TY_PTR) || (node_kind[ty] == TY_STRING))
    return 1;
  else {
  }
  return 0;
}
int tc_check_ffi_return_type(int ty) {
  if ((ty == 0) || (node_kind[ty] == TY_ARRAY))
    return 0;
  else {
  }
  return tc_check_ffi_type(ty);
}
int tc_program(int root) {
  tc_root = root;
  tc_ok = 1;
  tc_error_code = 0;
  tc_error_pos = (0 - 1);
  tc_error_expected_kind = 0;
  tc_error_found_kind = 0;
  tc_var_count = 0;
  tc_scope_count = 0;
  tc_path_count = 0;
  tc_loop_depth = 0;
  tc_flow_depth = 0;
  tc_flow_log_count = 0;
  (void)(tc_prepare_return_contracts(root));
  int collision_item = node_a[root];
  while (collision_item != 0) {
    if (((node_kind[collision_item] == N_FUNC) || (node_kind[collision_item] == N_GENERIC_FUNC)) &&
        (tc_reserved_function(node_value[collision_item]) == 1)) {
      tc_error_code = 43;
      tc_error_pos = node_pos[collision_item];
      tc_ok = 0;
      (void)(tc_diag());
      return 0;
    } else {
    }
    collision_item = node_next[collision_item];
  }
  if (tc_check_function_symbols(root) == 0) {
    (void)(tc_diag());
    return 0;
  } else {
  }
  (void)(tc_enter_scope());
  int item = node_a[root];
  while (item != 0) {
    if (node_kind[item] == N_STRUCT) {
      int f = node_a[item];
      while (f != 0) {
        (void)(tc_check_type(node_b[f]));
        f = node_next[f];
      }
      if (tc_cycle_struct(node_value[item]) == 1)
        (void)(tc_fail(28));
      else {
      }
    } else {
    }
    item = node_next[item];
  }
  item = node_a[root];
  while (item != 0) {
    if (node_kind[item] == N_EXTERN) {
      int header_id = node_a[item];
      if (header_id != 0) {
        if (ffi_header_valid(header_id) == 0) {
          if (tc_ok == 1) {
            tc_error_pos = node_pos[item];
            (void)(tc_fail(57));
          } else {
          }
        } else {
        }
      } else {
      }
      int ffi_param = node_c[item];
      while (ffi_param != 0) {
        int ffi_mode = node_aux[ffi_param];
        if (ffi_mode == 1) {
          if (tc_ok == 1) {
            tc_error_pos = node_pos[ffi_param];
            (void)(tc_fail(65));
          } else {
          }
        } else if (((ffi_mode == 2) || (ffi_mode == 3)) &&
                   (tc_ffi_pointer_compatible(node_b[ffi_param]) == 0)) {
          if (tc_ok == 1) {
            tc_error_pos = node_pos[ffi_param];
            (void)(tc_fail(66));
          } else {
          }
        } else {
        }
        if (tc_check_ffi_type(node_b[ffi_param]) == 0) {
          if (tc_ok == 1) {
            tc_error_pos = node_pos[ffi_param];
            (void)(tc_fail(55));
          } else {
          }
        } else {
        }
        ffi_param = node_next[ffi_param];
      }
      tc_path_count = 0;
      if (tc_check_ffi_return_type(node_b[item]) == 0) {
        if (tc_ok == 1) {
          tc_error_pos = node_pos[item];
          (void)(tc_fail(56));
        } else {
        }
      } else {
      }
    } else {
    }
    item = node_next[item];
  }
  if (tc_ok == 0) {
    (void)(tc_diag());
    return 0;
  } else {
  }
  item = node_a[root];
  while (item != 0) {
    if ((node_kind[item] == N_GLOBAL) || (node_kind[item] == N_CONST)) {
      (void)(tc_type_node(node_b[item]));
      int gk = tc_kind;
      int gn = tc_name;
      int ge = tc_elem_kind;
      int gen = tc_elem_name;
      (void)(tc_add_var(node_a[item], gk, gn, ge, gen, node_b[item]));
      if (node_kind[item] == N_CONST) {
        sym_type[node_a[item]] = (gk + 100);
        tc_var_const[tc_last_var_index] = 1;
      } else {
      }
      (void)(tc_mark_float_expr(node_c[item], gk));
      (void)(tc_expr(node_c[item]));
      int global_fun_target = tc_expr_fun_target;
      tc_var_fun_target[tc_last_var_index] = global_fun_target;
      if (tc_literal_fits(node_c[item], gk) == 0)
        (void)(tc_fail(54));
      else {
      }
      if ((((tc_same_full(gk, gn, ge, gen, tc_kind, tc_name, tc_elem_kind, tc_elem_name) == 0) &&
            (tc_integer_literal_conversion(node_c[item], gk, tc_kind) == 0)) &&
           ((node_kind[node_c[item]] != N_INT) || (node_value[node_c[item]] != 0))) &&
          (node_kind[node_c[item]] != N_NULL))
        (void)(tc_fail(29));
      else {
      }
    } else {
    }
    item = node_next[item];
  }
  tc_global_count = tc_var_count;
  item = node_a[root];
  while (item != 0) {
    if ((node_kind[item] == N_FUNC) || (node_kind[item] == N_GENERIC_FUNC)) {
      tc_var_count = tc_global_count;
      tc_scope_count = 1;
      (void)(tc_enter_scope());
      int p = node_c[item];
      int param_pos = 1;
      while (p != 0) {
        (void)(tc_type_node(node_b[p]));
        int pk = tc_kind;
        int pn = tc_name;
        int pek = tc_elem_kind;
        int pen = tc_elem_name;
        (void)(tc_add_var(node_a[p], pk, pn, pek, pen, node_b[p]));
        tc_var_param[tc_last_var_index] = 1;
        tc_var_param_pos[tc_last_var_index] = param_pos;
        tc_var_mode[tc_last_var_index] = node_aux[p];
        if ((node_aux[p] == 2) || (node_aux[p] == 3)) {
          tc_var_borrow_source[tc_last_var_index] = tc_last_var_index;
          tc_var_borrow_mode[tc_last_var_index] = node_aux[p];
          tc_var_borrow_param[tc_last_var_index] = param_pos;
        } else {
        }
        if (node_aux[p] == 1)
          tc_var_owned[tc_last_var_index] = 1;
        else if (node_aux[p] == 3)
          tc_var_borrow_mut[tc_last_var_index] = 1;
        else if (tc_is_owner_kind(pk) == 1)
          tc_var_owned[tc_last_var_index] = 1;
        else {
        }
        param_pos = (param_pos + 1);
        p = node_next[p];
      }
      (void)(tc_type_node(node_b[item]));
      tc_expected_elem_kind = tc_elem_kind;
      tc_expected_elem_name = tc_elem_name;
      (void)(tc_stmt(node_a[item], tc_kind, tc_name));
      (void)(tc_leave_scope());
    } else {
    }
    item = node_next[item];
  }
  if (tc_ok == 0) {
    (void)(tc_diag());
    return 0;
  } else {
  }
  return 1;
}
int pipeline_main(char *path) {
  (void)(load_tokens_from_file(path));
  node_count = 1;
  ast_parse_mode = 1;
  int root = ast_program();
  ast_parse_mode = 0;
  pipeline_root = root;
  int parsed = 1;
  if (lexer_error == 1)
    parsed = 0;
  else {
  }
  if (root < 0)
    parsed = 0;
  else {
  }
  if (input_peek() != T_EOF)
    parsed = 0;
  else {
  }
  if (include_ok == 0)
    parsed = 0;
  else {
  }
  if (parsed == 1) {
    if (tc_program(root) == 0)
      parsed = 0;
    else {
    }
  } else {
  }
  if (parsed == 1) {
    (void)(code_reset());
    (void)(gen_program(root));
    (void)(code_reset());
    (void)(gen_program(root));
    int stable_count = code_count;
    (void)(code_reset());
    (void)(gen_program(root));
    if (code_count != stable_count)
      parsed = 0;
    else {
    }
  } else {
  }
  if (parsed == 0) {
    if (source_import_error_kind != 0)
      (void)(source_import_diag());
    else if (tc_error_code == 0) {
      (void)(runtime_write_string("parse error\n"));
      (void)(runtime_write_string("diagnostic.code=0\n"));
      (void)(runtime_write_string("diagnostic.file="));
      (void)(tc_print_source_file(tc_diag_file(source_pos)));
      (void)(runtime_write_char(10));
      (void)(runtime_write_string("diagnostic.line="));
      (void)(runtime_write_int(tc_diag_line(source_pos)));
      (void)(runtime_write_char(10));
      (void)(runtime_write_string("diagnostic.column="));
      (void)(runtime_write_int(tc_diag_col(source_pos)));
      (void)(runtime_write_char(10));
      (void)(runtime_write_string(
          "diagnostic.hint=check the syntax near the reported source location\n"));
      (void)(runtime_write_string("diagnostic.excerpt="));
      (void)(tc_print_source_excerpt(source_pos));
      (void)(runtime_write_char(10));
    } else {
    }
  } else {
  }
  if (parsed == 1) {
    return 0;
  } else {
  }
  return 1;
}
void emit_symbol(int *out, int id) {
  int i = 0;
  while (i < sym_len[id]) {
    (void)(write_char(out, source[(sym_start[id] + i)]));
    i = (i + 1);
  }
}
void emit_string(int *out, int id) {
  (void)(write_char(out, 34));
  (void)(emit_symbol(out, id));
  (void)(write_char(out, 34));
}
void emit_identifier(int *out, int id) {
  int is_stdout = 0;
  int is_stderr = 0;
  if (sym_len[id] == 6) {
    if ((((((source[sym_start[id]] == 115) && (source[(sym_start[id] + 1)] == 116)) &&
           (source[(sym_start[id] + 2)] == 100)) &&
          (source[(sym_start[id] + 3)] == 111)) &&
         (source[(sym_start[id] + 4)] == 117)) &&
        (source[(sym_start[id] + 5)] == 116))
      is_stdout = 1;
    else {
    }
    if ((((((source[sym_start[id]] == 115) && (source[(sym_start[id] + 1)] == 116)) &&
           (source[(sym_start[id] + 2)] == 100)) &&
          (source[(sym_start[id] + 3)] == 101)) &&
         (source[(sym_start[id] + 4)] == 114)) &&
        (source[(sym_start[id] + 5)] == 114))
      is_stderr = 1;
    else {
    }
  } else {
  }
  (void)(emit_symbol(out, id));
  if ((is_stdout == 1) || (is_stderr == 1))
    (void)(write_char(out, 95));
  else {
  }
}
void emit_print_prefix(int *out) {
  (void)(write_string(out, "("));
  (void)(write_char(out, 34));
  (void)(write_string(out, "%d"));
  (void)(write_char(out, 92));
  (void)(write_char(out, 110));
  (void)(write_char(out, 34));
  (void)(write_string(out, ", "));
}
void emit_int_text(int *out, int value) {
  if (value < 0) {
    (void)(write_char(out, 45));
    (void)(emit_int_text(out, (0 - value)));
  } else if (value < 10) {
    (void)(write_char(out, (48 + value)));
  } else {
    (void)(emit_int_text(out, (value / 10)));
    (void)(write_char(out, (48 + (value - ((value / 10) * 10)))));
  }
}
void emit_source_filename(int *out, int file_id) {
  int i = 0;
  while (i < source_file_name_len[file_id]) {
    int c = source_file_name_text[(source_file_name_start[file_id] + i)];
    if ((c == 34) || (c == 92)) {
      (void)(write_char(out, 92));
      (void)(write_char(out, c));
    } else if (c == 10) {
      (void)(write_char(out, 92));
      (void)(write_char(out, 110));
    } else if (c == 13) {
      (void)(write_char(out, 92));
      (void)(write_char(out, 114));
    } else if (c == 9) {
      (void)(write_char(out, 92));
      (void)(write_char(out, 116));
    } else
      (void)(write_char(out, c));
    i = (i + 1);
  }
}
void emit_source_line(int *out, int pos) {
  if ((pos < 0) || (pos > (source_len - 1)))
    return;
  else {
  }
  int file_id = source_file_at[pos];
  if ((file_id < 1) || (file_id > (source_file_count - 1)))
    return;
  else {
  }
  (void)(write_char(out, 10));
  (void)(write_string(out, "#line "));
  (void)(emit_int_text(out, source_line_at[pos]));
  (void)(write_string(out, " \""));
  (void)(emit_source_filename(out, file_id));
  (void)(write_string(out, "\"\n"));
}
void emit_c_token(int *out, int kind, int value) {
  if (emit_pending_space == 1) {
    if ((((((kind == C_KW) || (kind == C_IDENT)) || (kind == C_INT)) || (kind == C_STRING)) ||
         (kind == C_RAW)) ||
        (kind == C_RAW_U64))
      (void)(write_char(out, 32));
    else {
    }
    emit_pending_space = 0;
  } else {
  }
  if (kind == C_KW) {
    if (value == 1) {
      (void)(write_string(out, "int"));
      emit_pending_space = 1;
    } else if (value == 2) {
      (void)(write_string(out, "int"));
      emit_pending_space = 1;
    } else if (value == 3) {
      (void)(write_string(out, "char*"));
      emit_pending_space = 1;
    } else if (value == 4) {
      (void)(write_string(out, "void"));
      emit_pending_space = 1;
    } else if (value == 5) {
      (void)(write_string(out, "return"));
      emit_pending_space = 1;
    } else if (value == 6) {
      (void)(write_string(out, "if"));
      emit_pending_space = 1;
    } else if (value == 7) {
      (void)(write_string(out, "else"));
      emit_pending_space = 1;
    } else if (value == 8) {
      (void)(write_string(out, "while"));
      emit_pending_space = 1;
    } else if (value == 9) {
      (void)(write_string(out, "break"));
      emit_pending_space = 1;
    } else if (value == 10) {
      (void)(write_string(out, "continue"));
      emit_pending_space = 1;
    } else if (value == 11) {
      (void)(write_string(out, "for"));
      emit_pending_space = 1;
    } else if (value == 12) {
      (void)(write_string(out, "struct"));
      emit_pending_space = 1;
    } else if (value == 13) {
      (void)(write_string(out, "enum"));
      emit_pending_space = 1;
    } else if (value == 14) {
      (void)(write_string(out, "typedef"));
      emit_pending_space = 1;
    } else if (value == 15) {
      (void)(write_string(out, "double"));
      emit_pending_space = 1;
    } else if (value == 16) {
      (void)(write_string(out, "const"));
      emit_pending_space = 1;
    } else if (value == 18) {
      (void)(write_string(out, "float"));
      emit_pending_space = 1;
    } else if (value == 19) {
      (void)(write_string(out, "long"));
      emit_pending_space = 1;
    } else if (value == 20) {
      (void)(write_string(out, "long long"));
      emit_pending_space = 1;
    } else if (value == 21) {
      (void)(write_string(out, "uint8_t"));
      emit_pending_space = 1;
    } else if (value == 22) {
      (void)(write_string(out, "uint16_t"));
      emit_pending_space = 1;
    } else if (value == 23) {
      (void)(write_string(out, "uint32_t"));
      emit_pending_space = 1;
    } else if (value == 24) {
      (void)(write_string(out, "uint64_t"));
      emit_pending_space = 1;
    } else if (value == 25) {
      (void)(write_string(out, "int8_t"));
      emit_pending_space = 1;
    } else if (value == 26) {
      (void)(write_string(out, "int16_t"));
      emit_pending_space = 1;
    } else if (value == 27) {
      (void)(write_string(out, "int32_t"));
      emit_pending_space = 1;
    } else if (value == 28) {
      (void)(write_string(out, "int64_t"));
      emit_pending_space = 1;
    } else if (value == 29) {
      (void)(write_string(out, "size_t"));
      emit_pending_space = 1;
    } else if (value == 30) {
      (void)(write_string(out, "union"));
      emit_pending_space = 1;
    } else if (value == 17) {
      (void)(write_string(out, "char"));
      emit_pending_space = 1;
    } else {
    }
  } else if (kind == C_IDENT) {
    if (value == (0 - 1001))
      (void)(write_string(out, "printf"));
    else if (value == (0 - 1002))
      (void)(write_string(out, "runtime_string_concat"));
    else if (value == (0 - 1011))
      (void)(write_string(out, "sizeof"));
    else if (value == (0 - 1012))
      (void)(write_string(out, "len"));
    else if (value == (0 - 1013))
      (void)(write_string(out, "cap"));
    else if (value == (0 - 1015))
      (void)(write_string(out, "data"));
    else if (value == (0 - 1016))
      (void)(write_string(out, "basalt_memory_alloc"));
    else if (value == (0 - 1017))
      (void)(write_string(out, "basalt_memory_resize"));
    else if (value == (0 - 1018))
      (void)(write_string(out, "basalt_memory_free"));
    else if (value == (0 - 1019))
      (void)(write_string(out, "basalt_memory_alloc_aligned"));
    else if (value == (0 - 1020))
      (void)(write_string(out, "_Alignas"));
    else if (value == (0 - 1021))
      (void)(write_string(out, "f"));
    else
      (void)(emit_identifier(out, value));
  } else if (kind == C_INT)
    (void)(emit_int_text(out, value));
  else if (kind == C_RAW)
    (void)(emit_symbol(out, value));
  else if (kind == C_RAW_U64) {
    (void)(emit_symbol(out, value));
    (void)(write_string(out, "ULL"));
  } else if (kind == C_STRING)
    (void)(emit_string(out, value));
  else if (kind == C_OP) {
    if (value == 1)
      (void)(write_string(out, "+"));
    else if (value == 2)
      (void)(write_string(out, "-"));
    else if (value == 3)
      (void)(write_string(out, "*"));
    else if (value == 4)
      (void)(write_string(out, "/"));
    else if (value == 5)
      (void)(write_string(out, "=="));
    else if (value == 6)
      (void)(write_string(out, "!="));
    else if (value == 7)
      (void)(write_string(out, "<"));
    else if (value == 8)
      (void)(write_string(out, ">"));
    else if (value == 9)
      (void)(write_string(out, "&&"));
    else if (value == 10)
      (void)(write_string(out, "||"));
    else if (value == 11)
      (void)(write_string(out, "++"));
    else if (value == 12)
      (void)(write_string(out, "&"));
    else if (value == 13)
      (void)(write_string(out, "|"));
    else if (value == 14)
      (void)(write_string(out, "^"));
    else if (value == 15)
      (void)(write_string(out, "<<"));
    else if (value == 16)
      (void)(write_string(out, ">>"));
    else if (value == 17)
      (void)(write_string(out, "%"));
    else if (value == 18)
      (void)(write_string(out, ":"));
    else if (value == 19)
      (void)(write_string(out, "+="));
    else if (value == 20)
      (void)(write_string(out, "-="));
    else if (value == 21)
      (void)(write_string(out, "*="));
    else if (value == 22)
      (void)(write_string(out, "/="));
    else if (value == 23)
      (void)(write_string(out, "%="));
    else if (value == 24)
      (void)(write_string(out, "&="));
    else if (value == 25)
      (void)(write_string(out, "|="));
    else if (value == 26)
      (void)(write_string(out, "^="));
    else if (value == 27)
      (void)(write_string(out, "<<="));
    else if (value == 28)
      (void)(write_string(out, ">>="));
    else if (value == 29)
      (void)(write_string(out, "!"));
    else if (value == 30)
      (void)(write_string(out, "<="));
    else if (value == 31)
      (void)(write_string(out, ">="));
    else {
    }
  } else if (kind == C_PUNCT) {
    if (value == 1)
      (void)(write_string(out, "*"));
    else if (value == 2)
      (void)(write_string(out, "["));
    else if (value == 3)
      (void)(write_string(out, "]"));
    else if (value == 4)
      (void)(write_string(out, "("));
    else if (value == 5)
      (void)(write_string(out, ")"));
    else if (value == 6)
      (void)(write_string(out, "("));
    else if (value == 7)
      (void)(write_string(out, ", "));
    else if (value == 8)
      (void)(write_string(out, ")"));
    else if (value == 9)
      (void)(write_string(out, "*"));
    else if (value == 10)
      (void)(write_string(out, "&"));
    else if (value == 11)
      (void)(write_string(out, " = "));
    else if (value == 12)
      (void)(write_string(out, ";"));
    else if (value == 13)
      (void)(write_string(out, "{\n"));
    else if (value == 14)
      (void)(write_string(out, "}\n"));
    else if (value == 15)
      (void)(emit_print_prefix(out));
    else if (value == 16) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%s"));
      (void)(write_char(out, 92));
      (void)(write_char(out, 110));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 17)
      (void)(write_string(out, "."));
    else if (value == 20) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%c"));
      (void)(write_char(out, 92));
      (void)(write_char(out, 110));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 21) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%g"));
      (void)(write_char(out, 92));
      (void)(write_char(out, 110));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 26) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%p"));
      (void)(write_char(out, 92));
      (void)(write_char(out, 110));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 18)
      (void)(write_string(out, " "));
    else if (value == 19)
      (void)(write_string(out, "{0}"));
    else if (value == 22)
      (void)(write_char(out, 32));
    else if (value == 23) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%d"));
      (void)(write_char(out, 92));
      (void)(write_char(out, 110));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 24)
      (void)(write_string(out, "{"));
    else if (value == 25)
      (void)(write_string(out, "}"));
    else if (value == 27)
      (void)(write_string(out, "->"));
    else if (value == 28) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%ld"));
      (void)(write_char(out, 92));
      (void)(write_char(out, 110));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 29) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%lld"));
      (void)(write_char(out, 92));
      (void)(write_char(out, 110));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 32) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%s"));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 33) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%c"));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 34) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%g"));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 35) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%ld"));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 36) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%lld"));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 37) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%p"));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else if (value == 38) {
      (void)(write_char(out, 40));
      (void)(write_char(out, 34));
      (void)(write_string(out, "%d"));
      (void)(write_char(out, 34));
      (void)(write_string(out, ", "));
    } else {
    }
  } else if (kind == C_NEWLINE)
    (void)(write_char(out, 10));
  else {
  }
}
void emit_runtime(int *out) {
  (void)(write_string(
      out,
      "#if defined(_WIN32)\n#include <direct.h>\n#else\n#define _POSIX_C_SOURCE 200809L\n#define "
      "_XOPEN_SOURCE 700\n#endif\n#include <stdio.h>\n#include <stdlib.h>\n#include "
      "<string.h>\n#include <stdint.h>\n#include <stddef.h>\n#include <limits.h>\n#include "
      "<errno.h>\n#include <stdatomic.h>\n#if defined(_WIN32)\n#include <windows.h>\n#include "
      "<process.h>\n#else\n#include <unistd.h>\n#include <fcntl.h>\n#include <poll.h>\n#include "
      "<sys/types.h>\n#include <sys/wait.h>\n#endif\n#if defined(_WIN32) && defined(__MINGW32__) "
      "&& !defined(BASALT_USE_NATIVE_C11_THREADS)\n#include <pthread.h>\n#include "
      "<sched.h>\ntypedef pthread_t thrd_t;\ntypedef int (*basalt_thrd_start_t)(void*);\ntypedef "
      "struct basalt_thrd_start_context { basalt_thrd_start_t entry; void* argument; int result; } "
      "basalt_thrd_start_context;\nstatic void* basalt_thrd_start(void* "
      "raw){basalt_thrd_start_context* "
      "context=(basalt_thrd_start_context*)raw;context->result=context->entry(context->argument);"
      "return raw;}\nstatic int basalt_thrd_create(thrd_t* thread,basalt_thrd_start_t entry,void* "
      "argument){basalt_thrd_start_context* context;int "
      "status;context=(basalt_thrd_start_context*)malloc(sizeof(*context));if(!context)return "
      "1;context->entry=entry;context->argument=argument;context->result=0;status=pthread_create("
      "thread,NULL,basalt_thrd_start,context);if(status!=0)free(context);return status;}\nstatic "
      "int basalt_thrd_join(thrd_t thread,int* result){void* raw=NULL;int "
      "status=pthread_join(thread,&raw);if(status==0){basalt_thrd_start_context* "
      "context=(basalt_thrd_start_context*)raw;if(result)*result=context->result;free(context);}"
      "return status;}\n#define thrd_success 0\n#define thrd_create(thread,entry,argument) "
      "basalt_thrd_create((thread),(entry),(argument))\n#define thrd_join(thread,result) "
      "basalt_thrd_join((thread),(result))\n#define thrd_yield() sched_yield()\n#else\n#include "
      "<threads.h>\n#endif\n#if defined(_WIN32) && defined(__MINGW32__)\n#ifndef "
      "BASALT_ALIGNED_ALLOC_DECLARED\n#define BASALT_ALIGNED_ALLOC_DECLARED 1\nvoid "
      "*aligned_alloc(size_t alignment,size_t size);\n#endif\n#endif\n#if defined(__GNUC__) || "
      "defined(__clang__)\n#define BASALT_UNUSED __attribute__((unused))\n#else\n#define "
      "BASALT_UNUSED\n#endif\nstatic void basalt_panic(int code){(void)code;exit(2);}\nstatic "
      "size_t basalt_checked_bytes(int count,size_t "
      "elem_size){if(count<0)basalt_panic(1);if(elem_size!=0&&(size_t)count>(size_t)-1/"
      "elem_size)basalt_panic(1);return(size_t)count*elem_size;}\nstatic void* "
      "basalt_track(void*);static void basalt_release(void*);\n"));
  (void)(write_string(
      out,
      "typedef struct basalt_atomic_int { atomic_int value; } basalt_atomic_int;\nstatic "
      "BASALT_UNUSED void* basalt_atomic_make(int "
      "initial){basalt_atomic_int*a=(basalt_atomic_int*)calloc(1,sizeof(*a));if(!a)basalt_panic(5);"
      "atomic_init(&a->value,initial);return basalt_track(a);}\nstatic BASALT_UNUSED int "
      "basalt_atomic_load(void*p){basalt_atomic_int*a=(basalt_atomic_int*)p;if(!a)basalt_panic(4);"
      "return atomic_load_explicit(&a->value,memory_order_acquire);}\nstatic BASALT_UNUSED void "
      "basalt_atomic_store(void*p,int "
      "value){basalt_atomic_int*a=(basalt_atomic_int*)p;if(!a)basalt_panic(4);atomic_store_"
      "explicit(&a->value,value,memory_order_release);}\nstatic BASALT_UNUSED int "
      "basalt_atomic_fetch_add(void*p,int "
      "delta){basalt_atomic_int*a=(basalt_atomic_int*)p;if(!a)basalt_panic(4);return "
      "atomic_fetch_add_explicit(&a->value,delta,memory_order_acq_rel);}\nstatic BASALT_UNUSED int "
      "basalt_atomic_compare_exchange(void*p,int expected,int "
      "desired){basalt_atomic_int*a=(basalt_atomic_int*)p;int "
      "old;if(!a)basalt_panic(4);old=expected;return "
      "atomic_compare_exchange_strong_explicit(&a->value,&old,desired,memory_order_acq_rel,memory_"
      "order_acquire);}\nstatic BASALT_UNUSED void "
      "basalt_atomic_free(void*p){basalt_release(p);}\ntypedef struct basalt_channel { _Atomic "
      "size_t head; _Atomic size_t tail; atomic_int closed; size_t capacity; int data[]; } "
      "basalt_channel;\nstatic BASALT_UNUSED void* basalt_channel_make(int requested){size_t "
      "cap=2;size_t "
      "bytes;basalt_channel*c;if(requested<1||requested>1073741824)basalt_panic(7);while(cap<(size_"
      "t)requested){if(cap>(size_t)-1/2)basalt_panic(1);cap*=2;}if(cap>(size_t)-1/"
      "sizeof(int))basalt_panic(1);bytes=sizeof(*c)+cap*sizeof(int);if(bytes<sizeof(*c))basalt_"
      "panic(1);c=(basalt_channel*)calloc(1,bytes);if(!c)basalt_panic(5);c->capacity=cap;atomic_"
      "init(&c->head,0);atomic_init(&c->tail,0);atomic_init(&c->closed,0);return "
      "basalt_track(c);}\nstatic BASALT_UNUSED int basalt_channel_send(void*p,int "
      "value){basalt_channel*c=(basalt_channel*)p;size_t "
      "head,tail;if(!c)basalt_panic(4);if(atomic_load_explicit(&c->closed,memory_order_acquire)!=0)"
      "return "
      "-1;head=atomic_load_explicit(&c->head,memory_order_relaxed);tail=atomic_load_explicit(&c->"
      "tail,memory_order_acquire);if(head-tail>=c->capacity)return "
      "0;c->data[head&(c->capacity-1)]=value;atomic_store_explicit(&c->head,head+1,memory_order_"
      "release);return 1;}\nstatic BASALT_UNUSED int "
      "basalt_channel_recv(void*p,int*out){basalt_channel*c=(basalt_channel*)p;size_t "
      "head,tail;if(!c||!out)basalt_panic(4);tail=atomic_load_explicit(&c->tail,memory_order_"
      "relaxed);head=atomic_load_explicit(&c->head,memory_order_acquire);if(tail==head){if(atomic_"
      "load_explicit(&c->closed,memory_order_acquire)!=0)return -1;return "
      "0;}*out=c->data[tail&(c->capacity-1)];atomic_store_explicit(&c->tail,tail+1,memory_order_"
      "release);return 1;}\nstatic BASALT_UNUSED void "
      "basalt_channel_close(void*p){basalt_channel*c=(basalt_channel*)p;if(!c)basalt_panic(4);"
      "atomic_store_explicit(&c->closed,1,memory_order_release);}\nstatic BASALT_UNUSED void "
      "basalt_channel_free(void*p){basalt_release(p);}\ntypedef struct basalt_thread_handle { "
      "thrd_t thread; } basalt_thread_handle;\nstatic BASALT_UNUSED void* "
      "basalt_thread_spawn(int(*entry)(void*),void*arg){basalt_thread_handle*h=(basalt_thread_"
      "handle*)calloc(1,sizeof(*h));if(!h)basalt_panic(5);if(thrd_create(&h->thread,entry,arg)!="
      "thrd_success){free(h);return NULL;}return basalt_track(h);}\nstatic BASALT_UNUSED int "
      "basalt_thread_join(void*p){basalt_thread_handle*h=(basalt_thread_handle*)p;int "
      "result;if(!h)basalt_panic(4);if(thrd_join(h->thread,&result)!=thrd_success)basalt_panic(8);"
      "basalt_release(h);return result;}\nstatic BASALT_UNUSED void "
      "basalt_thread_yield(void){thrd_yield();}\n"));
  (void)(write_string(
      out,
      "static char** basalt_inc_active=NULL;static size_t "
      "basalt_inc_active_n=0,basalt_inc_active_cap=0;static char** basalt_inc_loaded=NULL;static "
      "size_t basalt_inc_loaded_n=0,basalt_inc_loaded_cap=0;static char* "
      "basalt_inc_last_path=NULL;static char* basalt_inc_project_root=NULL;static int "
      "basalt_inc_status=0;\n"));
  (void)(write_string(out, "static BASALT_UNUSED int basalt_inc_eq(const char*a,const "
                           "char*b){return strcmp(a,b)==0;}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED size_t basalt_inc_find(char**v,size_t n,const char*p){size_t "
           "i;for(i=0;i<n;i++)if(basalt_inc_eq(v[i],p))return i;return (size_t)-1;}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED void basalt_inc_add(char***vp,size_t*np,size_t*cp,char*p){size_t "
           "c;char**q;if(*np==*cp){c=*cp?*cp*2:16;q=(char**)realloc(*vp,c*sizeof(char*));if(!q)"
           "exit(2);*vp=q;*cp=c;}(*vp)[(*np)++]=p;}\n"));
  (void)(write_string(out, "static BASALT_UNUSED char* basalt_inc_strdup(const char*p){size_t "
                           "n=strlen(p);char*q=(char*)malloc(n+1);if(!q)exit(2);memcpy(q,p,n+1);"
                           "return(char*)basalt_track(q);}\n"));
  (void)(write_string(out, "static BASALT_UNUSED char* basalt_inc_cwd(void){char buf[4096];\n#if "
                           "defined(_WIN32)\nif(_getcwd(buf,(int)sizeof(buf)))return "
                           "basalt_inc_strdup(buf);\n#else\nif(getcwd(buf,sizeof(buf)))return "
                           "basalt_inc_strdup(buf);\n#endif\nreturn basalt_inc_strdup(\"\");}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED char* basalt_inc_realpath(const "
           "char*p){if(p&&p[0]==0){if(basalt_inc_last_path)return "
           "basalt_inc_last_path;if(basalt_inc_active_n)return "
           "basalt_inc_active[basalt_inc_active_n-1];return basalt_inc_strdup(\"\");}\n#if "
           "defined(_WIN32)\nchar*q=_fullpath(NULL,p,0);if(q)return(char*)basalt_track(q);\n#"
           "else\nchar*q=realpath(p,NULL);if(q)return(char*)basalt_track(q);\n#endif\nreturn "
           "basalt_inc_strdup(p);}\n"));
  (void)(write_string(out, "static BASALT_UNUSED int "
                           "basalt_inc_begin(char*p){if(basalt_inc_find(basalt_inc_active,basalt_"
                           "inc_active_n,p)!=(size_t)-1){basalt_inc_status=1;return "
                           "0;}if(basalt_inc_find(basalt_inc_loaded,basalt_inc_loaded_n,p)!=(size_"
                           "t)-1){basalt_inc_status=2;return "
                           "0;}basalt_inc_add(&basalt_inc_active,&basalt_inc_active_n,&basalt_inc_"
                           "active_cap,p);basalt_inc_status=0;return 1;}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED void "
      "basalt_include_close(void){if(basalt_inc_active_n){char*p=basalt_inc_active[--basalt_inc_"
      "active_n];if(basalt_inc_find(basalt_inc_loaded,basalt_inc_loaded_n,p)==(size_t)-1)basalt_"
      "inc_add(&basalt_inc_loaded,&basalt_inc_loaded_n,&basalt_inc_loaded_cap,p);}}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED char* basalt_inc_join(const char*base,const char*raw){const "
           "char*s=strrchr(base,'/');size_t n=s?(size_t)(s-base+1):0;size_t "
           "m=strlen(raw);char*q=(char*)malloc(n+m+1);if(!q)exit(2);if(n)memcpy(q,base,n);memcpy(q+"
           "n,raw,m+1);return(char*)basalt_track(q);}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED char* basalt_inc_dir_join(const char*base,const char*raw){size_t "
           "n=strlen(base);size_t "
           "m=strlen(raw);char*q;if(n>(size_t)-1-m-2)exit(2);q=(char*)malloc(n+1+m+1);if(!q)exit(2)"
           ";memcpy(q,base,n);if(n&&base[n-1]!='/'&&base[n-1]!='\\\\')q[n++]='/"
           "';memcpy(q+n,raw,m+1);return(char*)basalt_track(q);}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED char* basalt_inc_expand_prefix(const char*raw){const "
      "char*base=NULL;size_t "
      "skip=0,n,m;char*q;if(strncmp(raw,\"@stdlib/\",8)==0){base=\"src/stdlib/\";skip=8;}else "
      "if(strncmp(raw,\"@lib/\",5)==0){base=\".basalt/vendor/\";skip=5;}else return "
      "NULL;n=strlen(base);m=strlen(raw+skip);if(n>(size_t)-1-m-1)exit(2);q=(char*)malloc(n+m+1);"
      "if(!q)exit(2);memcpy(q,base,n);memcpy(q+n,raw+skip,m+1);return(char*)basalt_track(q);}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED int basalt_inc_prefix_safe(const char*raw){const char*p=raw;const "
      "char*segment;size_t len;size_t skip=0;if(strncmp(raw,\"@stdlib/\",8)==0)skip=8;else "
      "if(strncmp(raw,\"@lib/\",5)==0)skip=5;else return "
      "1;segment=raw+skip;for(p=segment;;p++){if(*p==47||*p==92||*p==0){len=(size_t)(p-segment);if("
      "len==2&&segment[0]==46&&segment[1]==46)return 0;if(*p==0)return 1;segment=p+1;}}}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED int basalt_include_line_mode(int*line,int n){int "
           "i=0,j;while(i<n&&(line[i]==' "
           "'||line[i]==9))i++;if(i+7<=n&&line[i]=='i'&&line[i+1]=='n'&&line[i+2]=='c'&&line[i+3]=="
           "'l'&&line[i+4]=='u'&&line[i+5]=='d'&&line[i+6]=='e'){j=i+7;while(j<n&&(line[j]==' "
           "'||line[j]==9))j++;if(j<n&&line[j]==34)return "
           "1;}if(i+8<=n&&line[i]=='i'&&line[i+1]=='n'&&line[i+2]=='c'&&line[i+3]=='l'&&line[i+4]=="
           "'u'&&line[i+5]=='d'&&line[i+6]=='e'&&line[i+7]=='c'){j=i+8;while(j<n&&(line[j]==' "
           "'||line[j]==9))j++;if(j<n&&line[j]==34)return 2;}return 0;}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED void* basalt_include_open_root(const "
           "char*path){char*p=basalt_inc_realpath(path);FILE*f;basalt_inc_last_path=p;basalt_inc_"
           "project_root=basalt_inc_cwd();if(!basalt_inc_begin(p))return NULL;f=fopen(p,(const "
           "char[]){114,0});if(!f){basalt_inc_status=3;if(basalt_inc_active_n)basalt_inc_active_n--"
           ";return NULL;}return(void*)f;}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED void* basalt_include_open_line(int*line,int n,int mode){int "
           "i=0,a,b,j;char*raw,*joined,*canon,*expanded;FILE*f;(void)mode;basalt_inc_last_path="
           "basalt_inc_strdup(\"\");while(i<n&&line[i]!=34)i++;if(i>=n)return "
           "NULL;a=++i;while(i<n&&line[i]!=34)i++;if(i>=n)return "
           "NULL;b=i;raw=(char*)malloc((size_t)(b-a)+1);if(!raw)exit(2);{int "
           "k;for(k=0;k<b-a;k++)raw[k]=(char)line[a+k];}raw[b-a]=0;raw=(char*)basalt_track(raw);j="
           "i+1;while(j<n&&(line[j]==32||line[j]==9))j++;if(j<n&&line[j]==59)j++;while(j<n&&(line["
           "j]==32||line[j]==9))j++;if(j!=n)return "
           "NULL;expanded=basalt_inc_expand_prefix(raw);if(expanded){if(!basalt_inc_prefix_safe("
           "raw)){basalt_inc_status=4;return "
           "NULL;}joined=basalt_inc_dir_join(basalt_inc_project_root,expanded);canon=basalt_inc_"
           "realpath(joined);}else{joined=basalt_inc_join(basalt_inc_active[basalt_inc_active_n-1],"
           "raw);canon=basalt_inc_realpath(joined);}basalt_inc_last_path=canon;if(!basalt_inc_"
           "begin(canon))return NULL;f=fopen(canon,(const "
           "char[]){114,0});if(!f){basalt_inc_status=3;if(basalt_inc_active_n)basalt_inc_active_n--"
           ";return NULL;}return(void*)f;}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED int basalt_include_last_status(void){return basalt_inc_status;}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED void "
           "basalt_include_reset_session(void){basalt_inc_active_n=0;basalt_inc_loaded_n=0;basalt_"
           "inc_last_path=NULL;basalt_inc_project_root=NULL;basalt_inc_status=0;}\n"));
  (void)(write_string(out, "static BASALT_UNUSED void* open_file(const char* p,const char* "
                           "m){return (void*)fopen(p,m);}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED int read_char(void* h){int c=fgetc((FILE*)h);return c==EOF?-1:c;}\n"));
  (void)(write_string(out,
                      "static BASALT_UNUSED int close_file(void* h){return fclose((FILE*)h);}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED int write_char(void* h,int c){return fputc(c,(FILE*)h);}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED int write_string(void* h,const char* s){return fputs(s,(FILE*)h);}\n"));
  (void)(write_string(
      out, "static void** basalt_live=NULL;static size_t basalt_live_n=0,basalt_live_cap=0;\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED size_t basalt_find(void* p){size_t "
           "i;for(i=0;i<basalt_live_n;i++)if(basalt_live[i]==p)return i;return (size_t)-1;}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED void basalt_validate(void){size_t "
           "i,j;for(i=0;i<basalt_live_n;i++){if(!basalt_live[i])basalt_panic(2);for(j=i+1;j<basalt_"
           "live_n;j++)if(basalt_live[i]==basalt_live[j])basalt_panic(2);}}\n"));
  (void)(write_string(out,
                      "static BASALT_UNUSED void basalt_cleanup(void){size_t "
                      "i;basalt_validate();for(i=0;i<basalt_live_n;i++)free(basalt_live[i]);free("
                      "basalt_live);basalt_live=NULL;basalt_live_n=basalt_live_cap=0;}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED void* basalt_track(void* p){size_t c;void**q;if(!p)return "
           "NULL;if(basalt_find(p)!=(size_t)-1)basalt_panic(2);if(basalt_live_n==basalt_live_cap){"
           "if(basalt_live_cap>(size_t)-1/2)c=(size_t)-1;else "
           "c=basalt_live_cap?basalt_live_cap*2:32;if(c>(size_t)-1/"
           "sizeof(void*))basalt_panic(2);q=(void**)realloc(basalt_live,c*sizeof(void*));if(!q)"
           "basalt_panic(2);basalt_live=q;basalt_live_cap=c;}basalt_live[basalt_live_n++]=p;atexit("
           "basalt_cleanup);return p;}\n"));
  (void)(write_string(out, "static BASALT_UNUSED void basalt_release(void* p){size_t "
                           "i;if(!p)return;i=basalt_find(p);if(i==(size_t)-1)basalt_panic(2);free("
                           "p);basalt_live[i]=basalt_live[--basalt_live_n];}\n"));
  (void)(write_string(out, "static int basalt_io_status=0;\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED int runtime_io_status(void){return basalt_io_status;}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED char* runtime_read_line(int max_len){size_t n=0;int "
      "c=EOF;char*p;if(max_len<2||max_len>1048576)basalt_panic(7);p=(char*)malloc((size_t)max_len);"
      "if(!p)basalt_panic(5);while(n+1<(size_t)max_len){c=fgetc(stdin);if(c==EOF)break;if(c=='\\n')"
      "break;p[n++]=(char)c;}p[n]=0;if(c!=EOF&&c!='\\n'&&n+1==(size_t)max_len){basalt_io_status=3;"
      "do{c=fgetc(stdin);}while(c!=EOF&&c!='\\n');}else if(c==EOF&&n==0)basalt_io_status=1;else "
      "basalt_io_status=0;return(char*)basalt_track(p);}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED int runtime_read_int(int fallback){char buf[128];size_t n=0;int "
      "c=EOF;char*end;long "
      "v;while(n+1<sizeof(buf)){c=fgetc(stdin);if(c==EOF||c=='\\n')break;if(c!='\\r')buf[n++]=("
      "char)c;}buf[n]=0;if(c!=EOF&&c!='\\n'&&n+1==sizeof(buf)){basalt_io_status=3;do{c=fgetc(stdin)"
      ";}while(c!=EOF&&c!='\\n');return fallback;}if(c==EOF&&n==0){basalt_io_status=1;return "
      "fallback;}errno=0;v=strtol(buf,&end,10);while(*end==' "
      "'||*end=='\\t'||*end=='\\r')end++;if(end==buf||*end!=0){basalt_io_status=2;return "
      "fallback;}if(errno==ERANGE||v<(long)INT_MIN||v>(long)INT_MAX){basalt_io_status=4;return "
      "fallback;}basalt_io_status=0;return(int)v;}\n"));
  (void)(write_string(out, "static BASALT_UNUSED void runtime_write_string(const "
                           "char*s){if(!s)basalt_panic(4);if(fputs(s,stdout)==EOF||fflush(stdout)!="
                           "0)basalt_panic(8);}\n"));
  (void)(write_string(out, "static BASALT_UNUSED void runtime_write_line(const "
                           "char*s){if(!s)basalt_panic(4);if(fputs(s,stdout)==EOF||fputc('\\n',"
                           "stdout)==EOF||fflush(stdout)!=0)basalt_panic(8);}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED void runtime_write_int(int "
           "value){if(fprintf(stdout,\"%d\",value)<0||fflush(stdout)!=0)basalt_panic(8);}\n"));
  (void)(write_string(out,
                      "static BASALT_UNUSED void runtime_write_char(char value){if(fputc((unsigned "
                      "char)value,stdout)==EOF||fflush(stdout)!=0)basalt_panic(8);}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED void* basalt_memory_alloc(int count,size_t elem_size){size_t "
           "bytes=basalt_checked_bytes(count,elem_size);void*p=calloc(1,bytes?bytes:1);if(!p)"
           "basalt_panic(5);return basalt_track(p);}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED void* basalt_memory_alloc_aligned(int count,int alignment,size_t "
      "elem_size){size_t "
      "bytes,rounded,a;void*p;if(count<0||alignment<1)basalt_panic(1);a=(size_t)alignment;if((a&(a-"
      "1))!=0)basalt_panic(1);if(a<sizeof(void*))a=sizeof(void*);bytes=basalt_checked_bytes(count,"
      "elem_size);if(bytes==0)bytes=1;if(bytes>(size_t)-1-(a-1))basalt_panic(1);rounded=(bytes+a-1)"
      "&~(a-1);p=aligned_alloc(a,rounded);if(!p)basalt_panic(5);return basalt_track(p);}\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED void* basalt_memory_resize(void* old,int old_count,int "
      "new_count,size_t elem_size){size_t slot=(size_t)-1;size_t old_bytes;size_t "
      "new_bytes;void*p;if(old_count<0||new_count<0||new_count<old_count)basalt_panic(1);if(old){"
      "slot=basalt_find(old);if(slot==(size_t)-1)basalt_panic(2);}old_bytes=basalt_checked_bytes("
      "old_count,elem_size);new_bytes=basalt_checked_bytes(new_count,elem_size);p=realloc(old,new_"
      "bytes?new_bytes:1);if(!p)basalt_panic(6);if(slot==(size_t)-1)basalt_track(p);else "
      "basalt_live[slot]=p;if(new_bytes>old_bytes)memset((char*)p+old_bytes,0,new_bytes-old_bytes);"
      "return p;}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED void basalt_memory_free(void*p){basalt_release(p);}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED char* runtime_string_concat(const char* a,const char* b){size_t "
           "na,nb,total;char* "
           "p;if(!a||!b)basalt_panic(4);na=strlen(a);nb=strlen(b);if(na>(size_t)-1-nb-1)basalt_"
           "panic(1);total=na+nb+1;p=(char*)malloc(total);if(!p)basalt_panic(5);memcpy(p,a,na);"
           "memcpy(p+na,b,nb);p[na+nb]=0;return(char*)basalt_track(p);}\n"));
  (void)(write_string(
      out, "static char* basalt_sys_out=NULL;static char* basalt_sys_err=NULL;static int "
           "basalt_sys_status_value=0;static int basalt_sys_ok_value=0;static int "
           "basalt_sys_truncated_value=0;static int basalt_sys_spawn_error_value=0;static int "
           "basalt_sys_cleanup_registered=0;static void "
           "basalt_sys_free_buffers(void){if(basalt_sys_out){free(basalt_sys_out);basalt_sys_out="
           "NULL;}if(basalt_sys_err){free(basalt_sys_err);basalt_sys_err=NULL;}}\n"));
  (void)(write_string(out, "static BASALT_UNUSED char* "
                           "basalt_sys_empty(void){char*p=(char*)malloc(1);if(!p)basalt_panic(5);p["
                           "0]=0;return(char*)basalt_track(p);}\n"));
  (void)(write_string(out, "static BASALT_UNUSED char* "
                           "basalt_sys_raw_empty(void){char*p=(char*)malloc(1);if(!p)basalt_panic("
                           "5);p[0]=0;return p;}\n"));
  (void)(write_string(out, "static BASALT_UNUSED char* basalt_sys_copy_n(const char*s,size_t "
                           "n){char*p=(char*)malloc(n+1);if(!p)basalt_panic(5);if(n)memcpy(p,s,n);"
                           "p[n]=0;return(char*)basalt_track(p);}\n"));
  (void)(write_string(
      out,
      "#if defined(_WIN32)\nstatic BASALT_UNUSED void basalt_sys_drain(int fd,char*buf,size_t "
      "cap,size_t*len,int*truncated,int*open_flag){(void)fd;(void)buf;(void)cap;(void)len;(void)"
      "truncated;(void)open_flag;}\n#else\nstatic BASALT_UNUSED void basalt_sys_drain(int "
      "fd,char*buf,size_t cap,size_t*len,int*truncated,int*open_flag){char chunk[4096];ssize_t "
      "n;while((n=read(fd,chunk,sizeof(chunk)))>0){size_t "
      "take=0;if(*len<cap){take=(size_t)n;if(take>cap-*len)take=cap-*len;if(take)memcpy(buf+*len,"
      "chunk,take);*len+=take;}if((size_t)n>take)*truncated=1;}if(n==0||(n<0&&errno!=EAGAIN&&errno!"
      "=EWOULDBLOCK)){close(fd);*open_flag=0;}}\n#endif\n"));
  (void)(write_string(
      out, "#if defined(_WIN32)\ntypedef struct basalt_sys_windows_reader { HANDLE pipe; char "
           "*buffer; size_t cap; size_t len; int truncated; DWORD error; } "
           "basalt_sys_windows_reader;\nstatic BASALT_UNUSED int basalt_sys_windows_error(DWORD "
           "value){if(value==0)return 4;if(value>(DWORD)INT_MAX)return "
           "INT_MAX;return(int)value;}\nstatic BASALT_UNUSED int basalt_sys_utf8_to_wide(const "
           "char*value,wchar_t**result){int needed;wchar_t*buffer;if(!value||!result)return "
           "0;needed=MultiByteToWideChar(CP_UTF8,MB_ERR_INVALID_CHARS,value,-1,NULL,0);if(needed<="
           "0)return "
           "0;buffer=(wchar_t*)malloc((size_t)needed*sizeof(*buffer));if(!buffer)basalt_panic(5);"
           "if(MultiByteToWideChar(CP_UTF8,MB_ERR_INVALID_CHARS,value,-1,buffer,needed)!=needed){"
           "free(buffer);return 0;}*result=buffer;return 1;}\nstatic BASALT_UNUSED int "
           "basalt_sys_size_add(size_t*value,size_t add){if(add>(size_t)-1-*value)return "
           "0;*value+=add;return 1;}\nstatic BASALT_UNUSED int basalt_sys_quote_len(const "
           "wchar_t*value,size_t*result){size_t total=2;size_t slashes=0;const "
           "wchar_t*p;if(!value||!result)return "
           "0;for(p=value;*p!=L'\\0';p++){if(*p==L'\\\\'){if(slashes==(size_t)-1)return "
           "0;slashes++;}else{size_t add=slashes;if(*p==L'\"'){if(slashes>((size_t)-2)/2)return "
           "0;add=slashes*2+1;}if(!basalt_sys_size_add(&total,add))return "
           "0;slashes=0;}}if(slashes>((size_t)-1)/2)return "
           "0;if(!basalt_sys_size_add(&total,slashes*2))return 0;*result=total;return 1;}\nstatic "
           "BASALT_UNUSED void basalt_sys_quote_append(const "
           "wchar_t*value,wchar_t*out,size_t*pos){size_t slashes=0;const "
           "wchar_t*p;out[(*pos)++]=L'\"';for(p=value;*p!=L'\\0';p++){if(*p==L'\\\\'){slashes++;}"
           "else{size_t "
           "count=slashes;if(*p==L'\"')count=slashes*2+1;while(count>0){out[(*pos)++]=L'\\\\';"
           "count--;}out[(*pos)++]=*p;slashes=0;}}while(slashes>0){out[(*pos)++]=L'\\\\';out[(*pos)"
           "++]=L'\\\\';slashes--;}out[(*pos)++]=L'\"';}\nstatic DWORD WINAPI "
           "basalt_sys_windows_reader_thread(LPVOID "
           "raw){basalt_sys_windows_reader*reader=(basalt_sys_windows_reader*)raw;char "
           "chunk[4096];DWORD count;BOOL "
           "ok;for(;;){count=0;ok=ReadFile(reader->pipe,chunk,(DWORD)sizeof(chunk),&count,NULL);if("
           "!ok){DWORD "
           "error=GetLastError();if(error!=ERROR_BROKEN_PIPE)reader->error=error;break;}if(count=="
           "0)break;{size_t "
           "take=0;if(reader->len<reader->cap){take=(size_t)count;if(take>reader->cap-reader->len)"
           "take=reader->cap-reader->len;if(take)memcpy(reader->buffer+reader->len,chunk,take);"
           "reader->len+=take;}if((size_t)count>take)reader->truncated=1;}}CloseHandle(reader->"
           "pipe);reader->pipe=NULL;return 0;}\n#endif\n"));
  (void)(write_string(out, "#if defined(_WIN32)\n"));
  (void)(write_string(
      out,
      "static BASALT_UNUSED int basalt_sys_windows_run(const char*executable,char**args,int "
      "arg_count,size_t cap,char*out,char*err,int*truncated,int*spawn_error){HANDLE "
      "out_read=NULL,out_write=NULL,err_read=NULL,err_write=NULL,stdin_child=NULL;int "
      "stdin_owned=0;SECURITY_ATTRIBUTES security;STARTUPINFOEXW startup;PROCESS_INFORMATION "
      "process;wchar_t*exe_wide=NULL;wchar_t**wide_args=NULL;wchar_t*command_line=NULL;size_t "
      "command_length=0,item_length=0,pos=0;int i;int result=-1;int process_created=0;DWORD "
      "wait_result=WAIT_FAILED;DWORD exit_code=1;DWORD attribute_error=0;DWORD "
      "thread_error=0;SIZE_T attribute_size=0;LPPROC_THREAD_ATTRIBUTE_LIST attribute_list=NULL;int "
      "attribute_initialized=0;basalt_sys_windows_reader readers[2];HANDLE "
      "threads[2]={NULL,NULL};HANDLE inherited_handles[3]={NULL,NULL,NULL};const wchar_t "
      "nul_name[4]={78,85,76,0};memset(&security,0,sizeof(security));security.nLength=(DWORD)"
      "sizeof(security);security.bInheritHandle=TRUE;memset(&startup,0,sizeof(startup));memset(&"
      "process,0,sizeof(process));memset(readers,0,sizeof(readers));memset(inherited_handles,0,"
      "sizeof(inherited_handles));\n"));
  (void)(write_string(
      out, "if(!CreatePipe(&out_read,&out_write,&security,0)||!SetHandleInformation(out_read,"
           "HANDLE_FLAG_INHERIT,0)){goto "
           "windows_cleanup;}\nif(!CreatePipe(&err_read,&err_write,&security,0)||!"
           "SetHandleInformation(err_read,HANDLE_FLAG_INHERIT,0)){goto windows_cleanup;}\n{HANDLE "
           "current_input=GetStdHandle(STD_INPUT_HANDLE);if(current_input!=NULL&&current_input!="
           "INVALID_HANDLE_VALUE){if(!DuplicateHandle(GetCurrentProcess(),current_input,"
           "GetCurrentProcess(),&stdin_child,0,TRUE,DUPLICATE_SAME_ACCESS))stdin_child=NULL;}if("
           "stdin_child==NULL){stdin_child=CreateFileW(nul_name,GENERIC_READ,FILE_SHARE_READ|FILE_"
           "SHARE_WRITE,&security,OPEN_EXISTING,0,NULL);if(stdin_child==INVALID_HANDLE_VALUE)stdin_"
           "child=NULL;if(stdin_child==NULL)goto windows_cleanup; }stdin_owned=1;}\n"));
  (void)(write_string(
      out,
      "if(!basalt_sys_utf8_to_wide(executable,&exe_wide)){*spawn_error=ERROR_NO_UNICODE_"
      "TRANSLATION;goto "
      "windows_cleanup;}if(!basalt_sys_quote_len(exe_wide,&item_length)||!basalt_sys_size_add(&"
      "command_length,item_length)){*spawn_error=ERROR_NOT_ENOUGH_MEMORY;goto "
      "windows_cleanup;}if(arg_count>0){wide_args=(wchar_t**)calloc((size_t)arg_count,sizeof(*wide_"
      "args));if(!wide_args)basalt_panic(5);}for(i=0;i<arg_count;i++){if(!args||!args[i]||!basalt_"
      "sys_utf8_to_wide(args[i],&wide_args[i])){*spawn_error=ERROR_NO_UNICODE_TRANSLATION;goto "
      "windows_cleanup;}if(!basalt_sys_size_add(&command_length,1)||!basalt_sys_quote_len(wide_"
      "args[i],&item_length)||!basalt_sys_size_add(&command_length,item_length)){*spawn_error="
      "ERROR_NOT_ENOUGH_MEMORY;goto windows_cleanup;}}\n"));
  (void)(write_string(
      out,
      "if(command_length>(size_t)-1/sizeof(wchar_t)-1){*spawn_error=ERROR_NOT_ENOUGH_MEMORY;goto "
      "windows_cleanup;}command_line=(wchar_t*)malloc((command_length+1)*sizeof(*command_line));if("
      "!command_line)basalt_panic(5);basalt_sys_quote_append(exe_wide,command_line,&pos);for(i=0;i<"
      "arg_count;i++){command_line[pos++]=L' "
      "';basalt_sys_quote_append(wide_args[i],command_line,&pos);}command_line[pos]=L'\\0';"
      "inherited_handles[0]=stdin_child;inherited_handles[1]=out_write;inherited_handles[2]=err_"
      "write;if(InitializeProcThreadAttributeList(NULL,1,0,&attribute_size)){*spawn_error=4;goto "
      "windows_cleanup;}attribute_error=GetLastError();if(attribute_error!=ERROR_INSUFFICIENT_"
      "BUFFER||attribute_size==0){*spawn_error=basalt_sys_windows_error(attribute_error);goto "
      "windows_cleanup;}attribute_list=(LPPROC_THREAD_ATTRIBUTE_LIST)malloc(attribute_size);if(!"
      "attribute_list)basalt_panic(5);if(!InitializeProcThreadAttributeList(attribute_list,1,0,&"
      "attribute_size)){*spawn_error=basalt_sys_windows_error(GetLastError());goto "
      "windows_cleanup;}attribute_initialized=1;if(!UpdateProcThreadAttribute(attribute_list,0,"
      "PROC_THREAD_ATTRIBUTE_HANDLE_LIST,inherited_handles,(SIZE_T)(3*sizeof(inherited_handles[0]))"
      ",NULL,NULL)){*spawn_error=basalt_sys_windows_error(GetLastError());goto "
      "windows_cleanup;}startup.StartupInfo.cb=(DWORD)sizeof(startup);startup.StartupInfo.dwFlags="
      "STARTF_USESTDHANDLES;startup.StartupInfo.hStdInput=stdin_child;startup.StartupInfo."
      "hStdOutput=out_write;startup.StartupInfo.hStdError=err_write;startup.lpAttributeList="
      "attribute_list;if(!CreateProcessW(NULL,command_line,NULL,NULL,TRUE,CREATE_UNICODE_"
      "ENVIRONMENT|EXTENDED_STARTUPINFO_PRESENT,NULL,NULL,&startup.StartupInfo,&process)){*spawn_"
      "error=basalt_sys_windows_error(GetLastError());goto "
      "windows_cleanup;}process_created=1;CloseHandle(out_write);out_write=NULL;CloseHandle(err_"
      "write);err_write=NULL;CloseHandle(stdin_child);stdin_child=NULL;\n"));
  (void)(write_string(
      out,
      "readers[0].pipe=out_read;readers[0].buffer=out;readers[0].cap=cap;readers[0].len=0;readers["
      "0].truncated=0;readers[0].error=0;readers[1].pipe=err_read;readers[1].buffer=err;readers[1]."
      "cap=cap;readers[1].len=0;readers[1].truncated=0;readers[1].error=0;threads[0]=CreateThread("
      "NULL,0,basalt_sys_windows_reader_thread,&readers[0],0,NULL);if(!threads[0]){thread_error="
      "GetLastError();CloseHandle(out_read);out_read=NULL;readers[0].pipe=NULL;}threads[1]="
      "CreateThread(NULL,0,basalt_sys_windows_reader_thread,&readers[1],0,NULL);if(!threads[1]){if("
      "thread_error==0)thread_error=GetLastError();CloseHandle(err_read);err_read=NULL;readers[1]."
      "pipe=NULL;}if(thread_error!=0){*spawn_error=basalt_sys_windows_error(thread_error);"
      "TerminateProcess(process.hProcess,1);}wait_result=WaitForSingleObject(process.hProcess,"
      "INFINITE);if(wait_result!=WAIT_OBJECT_0&&*spawn_error==0)*spawn_error=basalt_sys_windows_"
      "error(GetLastError());if(threads[0]){WaitForSingleObject(threads[0],INFINITE);CloseHandle("
      "threads[0]);threads[0]=NULL;out_read=NULL;}if(threads[1]){WaitForSingleObject(threads[1],"
      "INFINITE);CloseHandle(threads[1]);threads[1]=NULL;err_read=NULL;}if(readers[0].error!=0&&*"
      "spawn_error==0)*spawn_error=basalt_sys_windows_error(readers[0].error);if(readers[1].error!="
      "0&&*spawn_error==0)*spawn_error=basalt_sys_windows_error(readers[1].error);*truncated="
      "readers[0].truncated||readers[1].truncated;out[readers[0].len]=0;err[readers[1].len]=0;if(*"
      "spawn_error==0&&!GetExitCodeProcess(process.hProcess,&exit_code))*spawn_error=basalt_sys_"
      "windows_error(GetLastError());if(*spawn_error==0){if(exit_code>(DWORD)INT_MAX)result=INT_"
      "MAX;else "
      "result=(int)exit_code;}CloseHandle(process.hThread);CloseHandle(process.hProcess);process_"
      "created=0;\n"));
  (void)(write_string(
      out,
      "windows_cleanup:basalt_sys_status_value=result;basalt_sys_ok_value=(result==0&&*spawn_error="
      "=0);if(process_created){TerminateProcess(process.hProcess,1);WaitForSingleObject(process."
      "hProcess,INFINITE);CloseHandle(process.hThread);CloseHandle(process.hProcess);}if(attribute_"
      "initialized){DeleteProcThreadAttributeList(attribute_list);attribute_initialized=0;}if("
      "attribute_list){free(attribute_list);attribute_list=NULL;}if(threads[0])CloseHandle(threads["
      "0]);if(threads[1])CloseHandle(threads[1]);if(out_read)CloseHandle(out_read);if(out_write)"
      "CloseHandle(out_write);if(err_read)CloseHandle(err_read);if(err_write)CloseHandle(err_write)"
      ";if(stdin_owned&&stdin_child)CloseHandle(stdin_child);if(command_line)free(command_line);if("
      "exe_wide)free(exe_wide);if(wide_args){for(i=0;i<arg_count;i++)if(wide_args[i])free(wide_"
      "args[i]);free(wide_args);}return result;}\n#endif\n"));
  (void)(write_string(
      out,
      "int basalt_sys_run(const char*executable,char**args,int arg_count,int max_output){size_t "
      "cap;\n#if !defined(_WIN32)\nsize_t out_len=0,err_len=0;int "
      "i;\n#endif\nbasalt_sys_free_buffers();if(!basalt_sys_cleanup_registered){atexit(basalt_sys_"
      "free_buffers);basalt_sys_cleanup_registered=1;}basalt_sys_status_value=-1;basalt_sys_ok_"
      "value=0;basalt_sys_truncated_value=0;basalt_sys_spawn_error_value=0;if(!executable||arg_"
      "count<0||max_output<0||max_output>16777216){basalt_sys_spawn_error_value=EINVAL;basalt_sys_"
      "out=basalt_sys_raw_empty();basalt_sys_err=basalt_sys_raw_empty();return "
      "-1;}cap=(size_t)max_output;basalt_sys_out=(char*)malloc(cap+1);basalt_sys_err=(char*)malloc("
      "cap+1);if(!basalt_sys_out||!basalt_sys_err)basalt_panic(5);basalt_sys_out[0]=0;basalt_sys_"
      "err[0]=0;\n#if defined(_WIN32)\nreturn "
      "basalt_sys_windows_run(executable,args,arg_count,cap,basalt_sys_out,basalt_sys_err,&basalt_"
      "sys_truncated_value,&basalt_sys_spawn_error_value);\n#else\n{int "
      "out_pipe[2],err_pipe[2],exec_pipe[2];pid_t child;int status;int out_open=1,err_open=1;int "
      "exec_error=0;ssize_t "
      "exec_read;if(pipe(out_pipe)!=0||pipe(err_pipe)!=0||pipe(exec_pipe)!=0){basalt_sys_spawn_"
      "error_value=errno;return "
      "-1;}if(fcntl(exec_pipe[1],F_SETFD,FD_CLOEXEC)<0){basalt_sys_spawn_error_value=errno;close("
      "out_pipe[0]);close(out_pipe[1]);close(err_pipe[0]);close(err_pipe[1]);close(exec_pipe[0]);"
      "close(exec_pipe[1]);return "
      "-1;}child=fork();if(child<0){basalt_sys_spawn_error_value=errno;close(out_pipe[0]);close("
      "out_pipe[1]);close(err_pipe[0]);close(err_pipe[1]);close(exec_pipe[0]);close(exec_pipe[1]);"
      "basalt_sys_out=basalt_sys_raw_empty();basalt_sys_err=basalt_sys_raw_empty();return "
      "-1;}if(child==0){char**av=(char**)calloc((size_t)arg_count+2,sizeof(char*));if(!av)_exit("
      "127);av[0]=(char*)executable;for(i=0;i<arg_count;i++)av[i+1]=args[i];av[arg_count+1]=NULL;"
      "close(out_pipe[0]);close(err_pipe[0]);close(exec_pipe[0]);if(dup2(out_pipe[1],STDOUT_FILENO)"
      "<0||dup2(err_pipe[1],STDERR_FILENO)<0){int "
      "e=errno;if(write(exec_pipe[1],&e,sizeof(e))<0){}_exit(127);}close(out_pipe[1]);close(err_"
      "pipe[1]);execvp(executable,av);{int "
      "e=errno;if(write(exec_pipe[1],&e,sizeof(e))<0){}}_exit(127);}close(out_pipe[1]);close(err_"
      "pipe[1]);close(exec_pipe[1]);if(fcntl(out_pipe[0],F_SETFL,O_NONBLOCK)<0||fcntl(err_pipe[0],"
      "F_SETFL,O_NONBLOCK)<0){basalt_sys_spawn_error_value=errno;}while(out_open||err_open){struct "
      "pollfd pf[2];int "
      "polled;pf[0].fd=out_open?out_pipe[0]:-1;pf[0].events=POLLIN;pf[1].fd=err_open?err_pipe[0]:-"
      "1;pf[1].events=POLLIN;polled=poll(pf,2,-1);if(polled<0){if(errno==EINTR)continue;break;}if("
      "out_open)basalt_sys_drain(out_pipe[0],basalt_sys_out,cap,&out_len,&basalt_sys_truncated_"
      "value,&out_open);if(err_open)basalt_sys_drain(err_pipe[0],basalt_sys_err,cap,&err_len,&"
      "basalt_sys_truncated_value,&err_open);}if(out_open){close(out_pipe[0]);out_open=0;}if(err_"
      "open){close(err_pipe[0]);err_open=0;}waitpid(child,&status,0);exec_read=read(exec_pipe[0],&"
      "exec_error,sizeof(exec_error));close(exec_pipe[0]);basalt_sys_out[out_len]=0;basalt_sys_err["
      "err_len]=0;if(exec_read>0){basalt_sys_spawn_error_value=exec_error;basalt_sys_status_value=-"
      "1;}else if(WIFEXITED(status))basalt_sys_status_value=WEXITSTATUS(status);else "
      "if(WIFSIGNALED(status))basalt_sys_status_value=0-WTERMSIG(status);else "
      "basalt_sys_status_value=-1;basalt_sys_ok_value=(basalt_sys_status_value==0&&basalt_sys_"
      "spawn_error_value==0);return basalt_sys_status_value;}\n#endif\n}\n"));
  (void)(write_string(
      out,
      "int basalt_sys_run_status(void){return basalt_sys_ok_value;}\nchar* "
      "basalt_sys_stdout(void){return basalt_sys_out?basalt_sys_out:basalt_sys_empty();}\nchar* "
      "basalt_sys_stderr(void){return basalt_sys_err?basalt_sys_err:basalt_sys_empty();}\nint "
      "basalt_sys_truncated(void){return basalt_sys_truncated_value;}\nint "
      "basalt_sys_spawn_error(void){return basalt_sys_spawn_error_value;}\n"));
  (void)(write_string(
      out, "int basalt_compile_argv(const char*cc,const char*input_c,const "
           "char*output_bin,char**all,int start,int extra_count){char**av;int "
           "i,status;if(!cc||!input_c||!output_bin||!all||start<0||extra_count<0)return "
           "-1;av=(char**)calloc((size_t)extra_count+4,sizeof(char*));if(!av)basalt_panic(5);for(i="
           "0;i<extra_count;i++)av[i]=all[start+i];av[extra_count]=(char*)input_c;av[extra_count+1]"
           "=(char*)\"-o\";av[extra_count+2]=(char*)output_bin;av[extra_count+3]=NULL;status="
           "basalt_sys_run(cc,av,extra_count+3,65536);if(status!=0&&basalt_sys_stderr()[0]!=0)"
           "fputs(basalt_sys_stderr(),stderr);free(av);return status;}\n"));
  (void)(write_string(
      out, "static BASALT_UNUSED int* alloc_ints(int n){int* "
           "p;if(n<0)basalt_panic(1);if(n<1)n=1;basalt_checked_bytes(n,sizeof(int));p=(int*)calloc("
           "(size_t)n,sizeof(int));if(!p)basalt_panic(5);return(int*)basalt_track(p);}\n"));
  (void)(write_string(out, "static BASALT_UNUSED void free_ints(int* p){basalt_release(p);}\n"));
  (void)(write_string(out,
                      "static BASALT_UNUSED int* grow_ints(int* p,int old,int n){size_t "
                      "slot=(size_t)-1;int* q;if(old<0||n<0)basalt_panic(1);if(n<=old)return "
                      "p;if(p){slot=basalt_find(p);if(slot==(size_t)-1)basalt_panic(2);}basalt_"
                      "checked_bytes(n,sizeof(int));q=(int*)realloc(p,(size_t)n*sizeof(int));if(!q)"
                      "basalt_panic(6);if(p)basalt_live[slot]=q;else "
                      "basalt_track(q);memset(q+old,0,(size_t)(n-old)*sizeof(int));return q;}\n"));
  (void)(write_string(
      out, "extern int* payload_int; extern int* payload_name; extern int* payload_string;\n"));
  (void)(write_string(out, "extern int* code_kind; extern int* code_value; extern int* input_kind; "
                           "extern int* input_value;\n"));
  (void)(write_string(
      out, "extern int* source; extern int* sym_start; extern int* sym_len; extern int* sym_hash; "
           "extern int* sym_kind; extern int* sym_type; extern int* sym_scope;\n\n"));
}
void emit_c_file(char *path) {
  int *out = open_file(path, "w");
  (void)(emit_runtime(out));
  int hi = 0;
  while (hi < ffi_header_count) {
    (void)(write_string(out, "#include \""));
    (void)(emit_symbol(out, ffi_header_ids[hi]));
    (void)(write_string(out, "\"\n"));
    hi = (hi + 1);
  }
  int ci = 0;
  while (ci < c_source_len) {
    (void)(write_char(out, c_source[ci]));
    ci = (ci + 1);
  }
  int i = 0;
  int last_epoch = (0 - 1);
  emit_pending_space = 0;
  while (i < code_count) {
    if (code_epoch[i] != last_epoch) {
      if (emit_line_directives == 1) {
        emit_pending_space = 0;
        (void)(emit_source_line(out, code_pos[i]));
      } else {
      }
      last_epoch = code_epoch[i];
    } else {
    }
    (void)(emit_c_token(out, code_kind[i], code_value[i]));
    i = (i + 1);
  }
  (void)(close_file(out));
}
int cli_arg_eq(char *a, char *b) {
  int i = 0;
  while (a[i] != 0) {
    if (b[i] == 0)
      return 0;
    else {
    }
    if (a[i] != b[i])
      return 0;
    else {
    }
    i = (i + 1);
  }
  if (b[i] != 0)
    return 0;
  else {
  }
  return 1;
}
int main(int argc, char **argv) {
  int input_index = (0 - 1);
  int output_index = (0 - 1);
  int auto_compile = 0;
  char *compiler = "cc";
  char *binary_path = "";
  int has_binary_path = 0;
  int extra_start = 0;
  int extra_count = 0;
  int i = 1;
  while (i < argc) {
    if (cli_arg_eq(argv[i], "--") == 1) {
      extra_start = (i + 1);
      extra_count = ((argc - i) - 1);
      i = argc;
    } else if (cli_arg_eq(argv[i], "--line") == 1) {
      emit_line_directives = 1;
      i = (i + 1);
    } else if (cli_arg_eq(argv[i], "--no-line") == 1) {
      emit_line_directives = 0;
      i = (i + 1);
    } else if (cli_arg_eq(argv[i], "--compile") == 1) {
      auto_compile = 1;
      i = (i + 1);
    } else if (cli_arg_eq(argv[i], "--cc") == 1) {
      if ((i + 1) >= argc)
        return 1;
      else {
      }
      compiler = argv[(i + 1)];
      i = (i + 2);
    } else if (cli_arg_eq(argv[i], "-o") == 1) {
      if ((i + 1) >= argc)
        return 1;
      else {
      }
      binary_path = argv[(i + 1)];
      has_binary_path = 1;
      i = (i + 2);
    } else if (input_index < 0) {
      input_index = i;
      i = (i + 1);
    } else if (output_index < 0) {
      output_index = i;
      i = (i + 1);
    } else {
      return 1;
    }
  }
  if (input_index < 0)
    return 1;
  else {
  }
  int ok = pipeline_main(argv[input_index]);
  if (ok != 0)
    return ok;
  else {
  }
  char *c_path = "";
  int has_c_path = 0;
  if (output_index >= 0) {
    c_path = argv[output_index];
    has_c_path = 1;
  } else if (auto_compile == 1) {
    c_path = runtime_string_concat(argv[input_index], ".c");
    has_c_path = 1;
  } else {
  }
  if (has_c_path == 1) {
    if (pipeline_root > 0) {
      (void)(gen_program(pipeline_root));
      (void)(emit_c_file(c_path));
    } else {
    }
  } else {
  }
  if (auto_compile == 1) {
    if (has_binary_path == 0)
      binary_path = runtime_string_concat(argv[input_index], ".out");
    else {
    }
    return basalt_compile_argv(compiler, c_path, binary_path, argv, extra_start, extra_count);
  } else {
  }
  return ok;
  return 0;
}
