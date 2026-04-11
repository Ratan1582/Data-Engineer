# Python for Data Engineering -- A Complete Deep Dive

**From Language Internals to Production Pipeline Patterns**

---

## Table of Contents

1. [How Python Executes Code](#1-how-python-executes-code)
2. [Memory Model](#2-memory-model)
3. [Garbage Collection](#3-garbage-collection)
4. [The GIL -- Global Interpreter Lock](#4-the-gil----global-interpreter-lock)
5. [Data Structures Internals](#5-data-structures-internals)
6. [Iterators, Generators, and Lazy Evaluation](#6-iterators-generators-and-lazy-evaluation)
7. [Concurrency and Parallelism](#7-concurrency-and-parallelism)
8. [Serialization](#8-serialization)
9. [Vectorization and Why Loops Are Slow](#9-vectorization-and-why-loops-are-slow)
10. [Decorators and Closures](#10-decorators-and-closures)
11. [Context Managers](#11-context-managers)
12. [Python Packaging and Environments](#12-python-packaging-and-environments)
13. [Memory Profiling and Optimization](#13-memory-profiling-and-optimization)
14. [Common Interview Patterns](#14-common-interview-patterns)
15. [Quick-Reference Cheat Sheet](#15-quick-reference-cheat-sheet)

---

## 1. How Python Executes Code

### 1.1 Compilation to Bytecode

Python is **not purely interpreted**. When you run `python script.py`, CPython (the standard interpreter) performs two steps:

```
Source code (.py)  -->  Compiler  -->  Bytecode (.pyc)  -->  PVM (Python Virtual Machine)  -->  Output
```

Step 1 is **compilation**: the source code is parsed into an AST (Abstract Syntax Tree), then compiled into **bytecode** -- a low-level, platform-independent set of instructions. This bytecode is cached in `__pycache__/` folders as `.pyc` files.

Step 2 is **interpretation**: the Python Virtual Machine (PVM) reads bytecode instructions one by one and executes them.

You can inspect bytecode with the `dis` module:

```python
import dis

def add(a, b):
    return a + b

dis.dis(add)
```

Output:
```
  2           0 LOAD_FAST                0 (a)
              2 LOAD_FAST                1 (b)
              4 BINARY_ADD
              6 RETURN_VALUE
```

Each line is a bytecode instruction. `LOAD_FAST` pushes a local variable onto the evaluation stack. `BINARY_ADD` pops two values, adds them, pushes the result. `RETURN_VALUE` returns the top of the stack.

### 1.2 Why This Matters for Data Engineering

Every Python operation has this overhead: bytecode dispatch, dynamic type checking, object creation. When you write:

```python
total = 0
for x in range(10_000_000):
    total += x
```

That loop executes 10 million iterations of: fetch `total`, fetch `x`, check types, call `int.__add__`, create new int object, bind result to `total`. Each iteration involves multiple bytecode dispatches and object allocations.

Compare with NumPy:

```python
import numpy as np
total = np.arange(10_000_000).sum()
```

This drops into compiled C code for the entire loop. No bytecode dispatch, no type checking per element, no new Python object per iteration. The difference: **~50x faster**.

This is the fundamental reason data engineers must understand Python internals -- to know when Python is appropriate and when to offload to compiled libraries or distributed frameworks like Spark.

---

## 2. Memory Model

### 2.1 Everything Is an Object

In Python, every value is an object on the heap. Even `int`, `float`, `bool`, and `None` are objects with headers.

A CPython object contains (at minimum):
- **Reference count** (8 bytes on 64-bit): number of references pointing to this object
- **Type pointer** (8 bytes): pointer to the object's type (e.g., `int`, `str`, `list`)
- **Value/payload**: the actual data

An integer in C uses 4 bytes. A Python `int` uses **28 bytes** minimum:

```python
import sys
sys.getsizeof(0)     # 28 bytes
sys.getsizeof(1)     # 28 bytes
sys.getsizeof(2**30) # 32 bytes (larger ints need more space)
sys.getsizeof("")    # 49 bytes (empty string)
sys.getsizeof([])    # 56 bytes (empty list)
sys.getsizeof({})    # 64 bytes (empty dict)
```

### 2.2 Variables Are References, Not Boxes

In Python, a variable is a **name** that references an object. It is not a storage box. This is critical to understand:

```python
a = [1, 2, 3]
b = a           # b references the SAME list object
b.append(4)
print(a)        # [1, 2, 3, 4] -- a is also modified
```

The assignment `b = a` does not copy the list. It creates a second reference to the same object in memory. Both `a` and `b` point to the same memory address:

```python
a = [1, 2, 3]
b = a
print(id(a) == id(b))  # True -- same object
```

### 2.3 Immutability vs Mutability

**Immutable objects**: `int`, `float`, `str`, `tuple`, `frozenset`, `bytes`
**Mutable objects**: `list`, `dict`, `set`, `bytearray`

When you "modify" an immutable object, Python creates a new object:

```python
x = 10
print(id(x))  # e.g., 140234567890
x += 1
print(id(x))  # different address -- new object created
```

This means string concatenation in a loop creates a new string object every iteration:

```python
# BAD: O(n^2) because each += creates a new string
result = ""
for word in words:
    result += word + " "

# GOOD: O(n) -- join builds the result once
result = " ".join(words)
```

### 2.4 Integer Caching

CPython caches small integers (-5 to 256) as singleton objects:

```python
a = 256
b = 256
print(a is b)  # True -- same cached object

a = 257
b = 257
print(a is b)  # False -- different objects (outside cache range)
```

This is an implementation detail, not a language guarantee. Never rely on `is` for integer comparison -- always use `==`.

### 2.5 String Interning

CPython also interns certain strings (identifiers, strings that look like variable names):

```python
a = "hello"
b = "hello"
print(a is b)  # True -- interned

a = "hello world!"
b = "hello world!"
print(a is b)  # False -- not interned (contains space and !)
```

### 2.6 `__slots__` -- Memory Optimization for Objects

By default, Python objects store attributes in a `__dict__` (a dict). For classes with many instances, this wastes memory:

```python
class PointDefault:
    def __init__(self, x, y):
        self.x = x
        self.y = y

class PointSlots:
    __slots__ = ('x', 'y')
    def __init__(self, x, y):
        self.x = x
        self.y = y

import sys
p1 = PointDefault(1, 2)
p2 = PointSlots(1, 2)
print(sys.getsizeof(p1) + sys.getsizeof(p1.__dict__))  # ~152 bytes
print(sys.getsizeof(p2))                                 # ~56 bytes
```

`__slots__` eliminates the per-instance `__dict__`, storing attributes in a fixed-size array. For millions of small objects, this cuts memory usage by 50-70%.

**Trade-off**: You cannot dynamically add attributes to slotted instances.

---

## 3. Garbage Collection

### 3.1 Reference Counting

CPython's primary garbage collection mechanism is **reference counting**. Every object has a counter tracking how many references point to it. When the count drops to zero, the object is immediately deallocated.

```python
a = [1, 2, 3]  # refcount = 1
b = a           # refcount = 2
del a           # refcount = 1
del b           # refcount = 0 --> immediately freed
```

Advantages:
- Deterministic: objects are freed the moment they become unreachable
- No pause times (unlike Java's GC)
- Simple to reason about

### 3.2 The Cycle Problem

Reference counting alone cannot handle **circular references**:

```python
class Node:
    def __init__(self):
        self.ref = None

a = Node()
b = Node()
a.ref = b  # a references b
b.ref = a  # b references a

del a
del b
# Both objects still have refcount=1 (from each other)
# Reference counting alone cannot free them
```

### 3.3 Generational Garbage Collector

To handle cycles, CPython has a **generational garbage collector** that runs periodically:

```
Generation 0 (youngest)  -->  survives collection  -->  promoted to Generation 1
Generation 1 (middle)    -->  survives collection  -->  promoted to Generation 2
Generation 2 (oldest)    -->  collected least frequently
```

- New objects start in Generation 0
- Generation 0 is collected most frequently (every ~700 allocations)
- Objects that survive move to Generation 1 (collected less often)
- Long-lived objects reach Generation 2 (collected rarely)

This is based on the **generational hypothesis**: most objects die young.

```python
import gc

gc.get_threshold()       # (700, 10, 10) -- default thresholds
gc.get_count()           # current allocation counts per generation
gc.collect()             # force a full collection
gc.disable()             # disable automatic collection (advanced use)
```

### 3.4 Weak References

When you want to reference an object without preventing its garbage collection:

```python
import weakref

class Cache:
    pass

obj = Cache()
weak = weakref.ref(obj)

print(weak())  # <Cache object>
del obj
print(weak())  # None -- object was garbage collected
```

Useful for caches, observer patterns, and avoiding memory leaks in frameworks.

---

## 4. The GIL -- Global Interpreter Lock

### 4.1 What It Is

The GIL is a mutex (mutual exclusion lock) in CPython that allows only **one thread to execute Python bytecode at a time**. Even on a 64-core machine, only one thread runs Python code at any instant.

```
Thread 1: [===RUN===]...[wait]...[===RUN===]...[wait]...
Thread 2: ...[wait]...[===RUN===]...[wait]...[===RUN===]
Thread 3: ...[wait]......[wait]...[===RUN===]...[wait]...
                         ^
                    Only one runs at a time
```

### 4.2 Why It Exists

The GIL protects CPython's **reference counting**. Without the GIL, two threads modifying the same object's reference count simultaneously would cause race conditions:

```
Thread A: read refcount (2) --> increment --> write (3)
Thread B: read refcount (2) --> increment --> write (3)
Actual should be: 4, but both threads read the old value
```

Possible outcomes without GIL: memory leaks (refcount too high), use-after-free (refcount drops to zero prematurely), segmentation faults.

### 4.3 When the GIL Releases

The GIL is not held constantly. It releases during:

1. **I/O operations**: file reads, network calls, database queries
2. **C extension calls**: NumPy, Pandas, OpenCV, scikit-learn operations that call into C
3. **`time.sleep()`**: explicitly yields the GIL
4. **Every N bytecode instructions**: CPython periodically releases (every 5ms by default in 3.2+)

```python
import threading
import time

# This DOES benefit from threads (I/O-bound)
def download(url):
    response = requests.get(url)  # GIL released during network I/O
    return response.text

# This does NOT benefit from threads (CPU-bound)
def compute(n):
    return sum(i * i for i in range(n))  # GIL held for entire computation
```

### 4.4 Implications for Data Engineering

| Workload Type | GIL Impact | Solution |
|---------------|-----------|----------|
| **I/O-bound** (file reads, API calls, DB queries) | Minimal -- GIL releases during I/O | Use `threading` or `asyncio` |
| **CPU-bound** (data transformation, computation) | Severe -- single-thread speed | Use `multiprocessing`, NumPy, or Spark |
| **Mixed** | Varies | Profile first, then choose |

This is why:
- **Pandas** is fast: operations drop into C code, releasing the GIL
- **Spark** bypasses the GIL: each executor runs a separate Python process
- **NumPy** arithmetic is fast: compiled C loops, GIL released

### 4.5 The `nogil` Future

PEP 703 (accepted in Python 3.13 as experimental) introduces a build mode where the GIL can be disabled. This uses per-object locks and biased reference counting instead of a global lock. It is still experimental and not the default.

---

## 5. Data Structures Internals

### 5.1 `dict` -- Hash Table

Python's `dict` is implemented as a **hash table** with open addressing.

How it works:
1. Compute `hash(key)` -- an integer
2. Compute `index = hash(key) % table_size` -- the slot
3. If the slot is empty, insert there
4. If occupied (collision), probe the next slot (using a perturbation scheme, not simple linear probing)

```python
d = {}
d["name"] = "Alice"  # hash("name") -> index -> store "Alice"
d["age"] = 30        # hash("age") -> index -> store 30

# Lookup:
print(d["name"])     # hash("name") -> same index -> return "Alice"
```

Time complexity:
- Average lookup/insert/delete: **O(1)**
- Worst case (all keys collide): **O(n)** -- practically never happens with good hash functions

Since Python 3.7, dicts **maintain insertion order** (guaranteed by the language spec). Internally, there are two arrays:
- A compact array storing `(hash, key, value)` entries in insertion order
- A sparse hash table of indices into the compact array

Memory: an empty dict uses 64 bytes. Each entry adds ~50-70 bytes.

### 5.2 `list` -- Dynamic Array

Python's `list` is a **dynamic array** (like `ArrayList` in Java or `vector` in C++).

Internally, it is a contiguous array of **pointers** to objects (not the objects themselves):

```
list object: [ptr0, ptr1, ptr2, ptr3, None, None, None, None]
                |     |     |     |
                v     v     v     v
              obj0  obj1  obj2  obj3
```

The array over-allocates to amortize append costs:

```python
import sys
sizes = []
lst = []
for i in range(20):
    lst.append(i)
    sizes.append(sys.getsizeof(lst))

# sizes: [88, 88, 88, 88, 120, 120, 120, 120, 184, ...]
# Notice jumps -- the list over-allocates when it grows
```

The growth pattern is roughly: 0, 4, 8, 16, 24, 32, 40, 52, 64, ... (grows by ~12.5% each time).

Time complexity:
| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `lst[i]` | O(1) | Direct index into pointer array |
| `lst.append(x)` | O(1) amortized | Occasionally O(n) when resizing |
| `lst.insert(0, x)` | O(n) | Must shift all elements right |
| `lst.pop()` | O(1) | Remove from end |
| `lst.pop(0)` | O(n) | Must shift all elements left |
| `x in lst` | O(n) | Linear scan |

### 5.3 `set` -- Hash Table Without Values

A `set` is essentially a `dict` with only keys (no values). Same hash table mechanism.

```python
s = {1, 2, 3, 4, 5}
print(3 in s)  # O(1) -- hash lookup
```

Key operations and complexity:
| Operation | Complexity |
|-----------|-----------|
| `x in s` | O(1) average |
| `s.add(x)` | O(1) average |
| `s & t` (intersection) | O(min(len(s), len(t))) |
| `s \| t` (union) | O(len(s) + len(t)) |
| `s - t` (difference) | O(len(s)) |

### 5.4 `tuple` -- Immutable Fixed Array

A `tuple` stores elements in a **fixed-size contiguous array**. No over-allocation, no resizing.

```python
import sys
sys.getsizeof((1, 2, 3))  # 64 bytes
sys.getsizeof([1, 2, 3])  # 120 bytes -- list is larger due to over-allocation
```

Tuples are slightly faster than lists for creation and access because:
- No over-allocation tracking
- CPython caches small tuples (length 0-20) for reuse
- Immutability allows certain compiler optimizations

### 5.5 `collections.deque` -- Double-Ended Queue

A `deque` is implemented as a **doubly-linked list of fixed-size blocks** (each block holds 64 items):

```python
from collections import deque

dq = deque()
dq.append(1)      # O(1) -- add to right
dq.appendleft(2)  # O(1) -- add to left
dq.pop()           # O(1) -- remove from right
dq.popleft()       # O(1) -- remove from left
```

Use `deque` instead of `list` when you need efficient operations on both ends. For a list, `insert(0, x)` and `pop(0)` are O(n).

### 5.6 `defaultdict`, `Counter`, `OrderedDict`

```python
from collections import defaultdict, Counter, OrderedDict

# defaultdict: auto-creates missing keys with a factory
word_count = defaultdict(int)
for word in words:
    word_count[word] += 1  # no KeyError even for new words

# Counter: specialized dict for counting
c = Counter(["a", "b", "a", "c", "a", "b"])
print(c.most_common(2))  # [('a', 3), ('b', 2)]

# OrderedDict: remembers insertion order (redundant since Python 3.7 for dict)
# Still useful for: move_to_end(), popitem(last=False), equality checks that consider order
```

---

## 6. Iterators, Generators, and Lazy Evaluation

### 6.1 The Iterator Protocol

Any object that implements `__iter__()` and `__next__()` is an iterator:

```python
class CountUp:
    def __init__(self, limit):
        self.limit = limit
        self.current = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.current >= self.limit:
            raise StopIteration
        self.current += 1
        return self.current

for num in CountUp(5):
    print(num)  # 1, 2, 3, 4, 5
```

Under the hood, `for x in iterable` does:
```python
iterator = iter(iterable)  # calls iterable.__iter__()
while True:
    try:
        x = next(iterator)  # calls iterator.__next__()
    except StopIteration:
        break
```

### 6.2 Generators -- Lazy Iterators

A generator function uses `yield` instead of `return`. It produces values one at a time, pausing execution between yields:

```python
def read_large_file(path):
    with open(path) as f:
        for line in f:
            yield line.strip()

# Memory usage: only ONE line in memory at a time
for line in read_large_file("10gb_file.csv"):
    process(line)
```

How it works internally:
1. Calling a generator function returns a **generator object** (does not execute the body)
2. Each `next()` call executes until the next `yield`, returns the yielded value, and **suspends** the function's state (local variables, instruction pointer)
3. The next `next()` resumes from where it left off
4. When the function returns (or ends), `StopIteration` is raised

```python
def gen():
    print("Step 1")
    yield 1
    print("Step 2")
    yield 2
    print("Step 3")

g = gen()           # Nothing printed -- generator created but not executed
next(g)             # Prints "Step 1", returns 1
next(g)             # Prints "Step 2", returns 2
next(g)             # Prints "Step 3", raises StopIteration
```

### 6.3 Generator Expressions

Like list comprehensions but lazy:

```python
# List comprehension -- ALL 10M items in memory
squares = [x**2 for x in range(10_000_000)]  # ~80 MB

# Generator expression -- ONE item at a time
squares = (x**2 for x in range(10_000_000))  # ~100 bytes
```

### 6.4 `yield from` -- Delegation

`yield from` delegates to a sub-generator:

```python
def read_all_files(paths):
    for path in paths:
        yield from read_large_file(path)

# Equivalent to:
def read_all_files(paths):
    for path in paths:
        for line in read_large_file(path):
            yield line
```

`yield from` is more efficient because it avoids the overhead of the outer `for` loop -- the Python interpreter handles the delegation internally.

### 6.5 Generator Pipelines

Chain generators to build memory-efficient processing pipelines:

```python
def read_csv(path):
    with open(path) as f:
        next(f)  # skip header
        for line in f:
            yield line.strip().split(",")

def filter_active(rows):
    for row in rows:
        if row[3] == "active":
            yield row

def extract_emails(rows):
    for row in rows:
        yield row[2]

# Pipeline: reads, filters, extracts -- only ONE row in memory
emails = extract_emails(filter_active(read_csv("users.csv")))
for email in emails:
    send_notification(email)
```

This is conceptually identical to how Spark's lazy transformations work -- no data flows until an action (like the `for` loop) pulls it through.

### 6.6 `itertools` -- The Standard Library Toolkit

```python
import itertools

# chain: concatenate iterables without copying
all_rows = itertools.chain(file1_rows, file2_rows, file3_rows)

# islice: take a slice without materializing the whole iterable
first_100 = itertools.islice(huge_generator, 100)

# groupby: group consecutive elements (must be sorted first)
for key, group in itertools.groupby(sorted_data, key=lambda x: x[0]):
    print(key, list(group))

# product: cartesian product
for combo in itertools.product([1, 2], ["a", "b"]):
    print(combo)  # (1, 'a'), (1, 'b'), (2, 'a'), (2, 'b')

# batched (Python 3.12+): split iterable into chunks
for batch in itertools.batched(range(10), 3):
    print(batch)  # (0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)
```

---

## 7. Concurrency and Parallelism

### 7.1 The Landscape

```
                        Concurrency Model
                              |
              +---------------+---------------+
              |               |               |
         Threading        Asyncio      Multiprocessing
         (concurrent,     (concurrent,    (true parallel,
          not parallel)    not parallel)    separate processes)
              |               |               |
         I/O-bound        I/O-bound      CPU-bound
         (simple API)     (high volume)   (computation)
```

### 7.2 Threading

```python
import threading
import time

def download(url, results, index):
    time.sleep(1)  # simulating I/O
    results[index] = f"Data from {url}"

urls = ["url1", "url2", "url3", "url4"]
results = [None] * len(urls)
threads = []

for i, url in enumerate(urls):
    t = threading.Thread(target=download, args=(url, results, i))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

# All 4 downloads complete in ~1 second (concurrent I/O)
```

Threading works for I/O because the GIL is released during I/O waits. But for CPU-bound work, threading is actually **slower** than single-threaded due to GIL contention and context-switching overhead.

### 7.3 Multiprocessing

```python
from multiprocessing import Pool

def compute_square(n):
    return n * n

with Pool(processes=4) as pool:
    results = pool.map(compute_square, range(1_000_000))
```

Each worker is a **separate Python process** with its own GIL, memory space, and interpreter. True parallelism, but:
- **Higher overhead**: process creation, inter-process communication (IPC)
- **Data copying**: arguments and results are serialized (pickled) between processes
- **No shared memory** (by default): must use `multiprocessing.Value`, `multiprocessing.Array`, or `shared_memory`

### 7.4 `concurrent.futures` -- High-Level API

```python
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# For I/O-bound work
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(download, url) for url in urls]
    results = [f.result() for f in futures]

# For CPU-bound work
with ProcessPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(heavy_compute, data) for data in chunks]
    results = [f.result() for f in futures]
```

### 7.5 Asyncio

```python
import asyncio
import aiohttp

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

asyncio.run(main())
```

Asyncio uses a **single-threaded event loop** with cooperative multitasking. When a coroutine hits `await`, it yields control back to the event loop, which can run another coroutine. No thread switching, no GIL issues.

Best for: **high-concurrency I/O** (thousands of simultaneous network connections). Not suitable for CPU-bound work.

### 7.6 Comparison Table

| Approach | True Parallelism | GIL Issue | Best For | Overhead |
|----------|-----------------|-----------|----------|----------|
| `threading` | No | Yes (CPU) | Simple I/O concurrency | Low |
| `asyncio` | No | No (single thread) | High-volume I/O (1000s of connections) | Very low |
| `multiprocessing` | Yes | No (separate processes) | CPU-bound computation | High (serialization, IPC) |
| **Spark** | Yes | No (separate processes + JVM) | Distributed, large-scale | Network + serialization |

### 7.7 How Spark Uses Python Concurrency

When you run a PySpark job:
1. The **driver** is a single Python process
2. It sends serialized (pickled) Python functions to the JVM
3. The JVM distributes work to **executors**
4. Each executor that needs to run Python code spawns a **Python worker process**
5. Data passes between JVM and Python via **sockets** (or Apache Arrow for Pandas UDFs)

This means each Spark task runs in its own Python process -- no GIL contention.

---

## 8. Serialization

### 8.1 What Is Serialization

Serialization converts an object in memory into a **byte stream** that can be stored on disk or sent over a network. Deserialization reverses the process.

```
Python Object  --serialize-->  Bytes  --deserialize-->  Python Object
```

### 8.2 `pickle` -- Python's Default

```python
import pickle

data = {"users": [{"name": "Alice", "age": 30}], "count": 1}

# Serialize
serialized = pickle.dumps(data)  # bytes

# Deserialize
restored = pickle.loads(serialized)
print(restored == data)  # True
```

Pickle handles nearly any Python object: lists, dicts, classes, functions (with caveats), closures, and nested structures.

**Pickle protocols** (higher = faster, smaller):
| Protocol | Python Version | Notes |
|----------|---------------|-------|
| 0 | All | ASCII format, human-readable, slow |
| 1 | All | Binary, smaller |
| 2 | 2.3+ | More efficient for new-style classes |
| 3 | 3.0+ | Default for Python 3, bytes support |
| 4 | 3.4+ | Supports very large objects (>4GB) |
| 5 | 3.8+ | Out-of-band data, zero-copy for buffers |

### 8.3 `cloudpickle` -- Extended Pickle

Standard `pickle` cannot serialize lambdas, closures, or functions defined interactively. `cloudpickle` can:

```python
import cloudpickle
import pickle

f = lambda x: x * 2

# pickle.dumps(f)  # Would fail for interactive lambdas
cloudpickle.dumps(f)  # Works
```

**This is what PySpark uses** to send Python functions from the driver to executors. When you write:

```python
df.filter(lambda row: row.age > 30)
```

PySpark serializes that lambda with `cloudpickle`, sends the bytes to each executor, and deserializes it there.

### 8.4 `json` -- Human-Readable, Language-Independent

```python
import json

data = {"name": "Alice", "age": 30, "scores": [95, 87, 92]}
json_str = json.dumps(data)   # '{"name": "Alice", "age": 30, "scores": [95, 87, 92]}'
restored = json.loads(json_str)
```

JSON only supports: `str`, `int`, `float`, `bool`, `None`, `list`, `dict`. No custom objects, no dates, no sets, no tuples (converted to lists).

### 8.5 Serialization Performance

```
Format        | Serialize Speed | Deserialize Speed | Size    | Human Readable
------------- | --------------- | ----------------- | ------- | --------------
pickle (5)    | Fast            | Fast              | Small   | No
cloudpickle   | Medium          | Medium            | Small   | No
json          | Medium          | Medium            | Medium  | Yes
msgpack       | Very fast       | Very fast         | Smaller | No
Arrow IPC     | Very fast       | Very fast (zero-copy) | Small | No
```

### 8.6 Why Serialization Matters for Spark

Every piece of data that crosses a boundary must be serialized:

```
Python Driver  --cloudpickle-->  JVM Driver  --Java serializer-->  JVM Executor  --socket-->  Python Worker
```

1. **UDFs**: The Python function is cloudpickled and sent to executors
2. **Data for UDFs**: Spark rows are serialized from JVM to Python format and back
3. **Broadcast variables**: The Python object is pickled and sent to every executor
4. **Pandas UDFs use Arrow**: This is much faster because Arrow is columnar and supports zero-copy

This is why native Spark functions (no Python UDF) are faster -- data stays in the JVM, no serialization boundary.

---

## 9. Vectorization and Why Loops Are Slow

### 9.1 Python Loop Overhead

A Python `for` loop executing a million iterations involves, per iteration:
1. Call `__next__()` on the iterator (bytecode dispatch + function call)
2. Type-check the operation (dynamic dispatch)
3. Create a new result object on the heap
4. Update reference counts
5. Execute the bytecode for the loop body

That is roughly **5-10 bytecode operations per iteration**, each requiring the interpreter's main loop to dispatch.

### 9.2 NumPy Vectorization

NumPy arrays store data in **contiguous C arrays** (not pointer arrays like Python lists):

```
Python list:  [ptr] -> [ptr] -> [ptr] -> [ptr]
                |        |        |        |
               obj0    obj1    obj2    obj3     (scattered in heap)

NumPy array:  [val0, val1, val2, val3]          (contiguous memory)
```

When you call `np.sum(arr)`, the C code:
1. Iterates over contiguous memory (cache-friendly)
2. No type checking per element (all same dtype)
3. No Python object creation per element
4. Can use SIMD instructions (process 4-8 values per CPU cycle)

### 9.3 Performance Comparison

```python
import numpy as np
import time

size = 10_000_000

# Python loop
py_list = list(range(size))
start = time.time()
result = sum(x * 2 for x in py_list)
py_time = time.time() - start

# NumPy vectorized
np_arr = np.arange(size)
start = time.time()
result = np.sum(np_arr * 2)
np_time = time.time() - start

print(f"Python loop: {py_time:.3f}s")  # ~1.5s
print(f"NumPy:       {np_time:.3f}s")  # ~0.02s
print(f"Speedup:     {py_time/np_time:.0f}x")  # ~75x
```

### 9.4 Pandas Vectorized Operations

Pandas is built on NumPy and inherits its vectorization:

```python
import pandas as pd

df = pd.DataFrame({"value": range(1_000_000)})

# SLOW: Python loop via apply
result = df["value"].apply(lambda x: x * 2 + 1)  # ~500ms

# FAST: Vectorized operation
result = df["value"] * 2 + 1                       # ~5ms (100x faster)
```

**Rule**: If you can express your operation as array arithmetic (element-wise operations, boolean indexing, string methods), use vectorized operations. Only fall back to `apply` when the logic is too complex.

### 9.5 When You Must Use Loops

Sometimes loops are unavoidable:
- **Stateful computations**: where the next value depends on the previous result
- **Complex branching**: many if/else conditions per element
- **External API calls**: per-element network requests

In these cases, minimize overhead:
```python
# Bad: Pandas apply (creates Series overhead per call)
df["result"] = df.apply(lambda row: complex_func(row["a"], row["b"]), axis=1)

# Better: Vectorized where possible, loop for the rest
mask = df["a"] > 0
df.loc[mask, "result"] = df.loc[mask, "a"] * df.loc[mask, "b"]
df.loc[~mask, "result"] = 0

# Best for complex logic: Use NumPy's vectorize (still has Python overhead per call
# but avoids Pandas Series overhead)
vfunc = np.vectorize(complex_func)
df["result"] = vfunc(df["a"].values, df["b"].values)
```

---

## 10. Decorators and Closures

### 10.1 First-Class Functions

In Python, functions are objects. You can assign them to variables, pass them as arguments, and return them from other functions:

```python
def greet(name):
    return f"Hello, {name}"

f = greet          # f now references the function object
print(f("Alice"))  # "Hello, Alice"
```

### 10.2 Closures

A closure is a function that **captures variables** from its enclosing scope:

```python
def make_multiplier(factor):
    def multiply(x):
        return x * factor  # 'factor' is captured from the outer scope
    return multiply

double = make_multiplier(2)
triple = make_multiplier(3)

print(double(5))  # 10
print(triple(5))  # 15
```

`multiply` is a closure -- it "closes over" the variable `factor`. Even after `make_multiplier` returns, `multiply` retains access to `factor` through the closure.

Closures capture the **variable itself** (the reference), not the value at the time of creation:

```python
def make_funcs():
    funcs = []
    for i in range(5):
        funcs.append(lambda: i)  # all capture the SAME variable 'i'
    return funcs

# All return 4 (the final value of i)
print([f() for f in make_funcs()])  # [4, 4, 4, 4, 4]

# Fix: capture the current value via default argument
def make_funcs_fixed():
    funcs = []
    for i in range(5):
        funcs.append(lambda i=i: i)  # default arg captures current value
    return funcs

print([f() for f in make_funcs_fixed()])  # [0, 1, 2, 3, 4]
```

### 10.3 Decorators

A decorator is a function that wraps another function to modify its behavior:

```python
import time
import functools

def timer(func):
    @functools.wraps(func)  # preserves func's __name__, __doc__, etc.
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"{func.__name__} took {elapsed:.4f}s")
        return result
    return wrapper

@timer
def slow_function(n):
    time.sleep(n)

slow_function(2)  # Prints: "slow_function took 2.0012s"
```

The `@timer` syntax is equivalent to `slow_function = timer(slow_function)`.

### 10.4 Decorator with Arguments

```python
def retry(max_attempts=3, delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        raise
                    print(f"Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry(max_attempts=5, delay=2)
def unreliable_api_call():
    # ...
    pass
```

### 10.5 Class-Based Decorators

```python
class CacheResult:
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        if args not in self.cache:
            self.cache[args] = self.func(*args)
        return self.cache[args]

@CacheResult
def fibonacci(n):
    if n < 2:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)

print(fibonacci(100))  # instant -- cached
```

### 10.6 `functools.lru_cache` -- Built-in Memoization

```python
from functools import lru_cache

@lru_cache(maxsize=128)
def expensive_query(user_id):
    return database.fetch(user_id)

# First call: hits database
# Second call with same user_id: returns cached result
```

---

## 11. Context Managers

### 11.1 The `with` Statement

```python
with open("data.csv") as f:
    data = f.read()
# f is automatically closed here, even if an exception occurred
```

Under the hood, `with` calls:
1. `__enter__()` -- setup (open file, acquire lock, start transaction)
2. The block body executes
3. `__exit__(exc_type, exc_val, exc_tb)` -- teardown (close file, release lock, commit/rollback)

### 11.2 Custom Context Managers

```python
class DatabaseConnection:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.connection = None

    def __enter__(self):
        self.connection = create_connection(self.connection_string)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()
        return False  # don't suppress exceptions

with DatabaseConnection("postgresql://...") as conn:
    conn.execute("INSERT INTO ...")
    conn.execute("UPDATE ...")
# auto-commits on success, auto-rollbacks on exception
```

### 11.3 `contextlib.contextmanager` -- Decorator-Based

```python
from contextlib import contextmanager

@contextmanager
def timer(label):
    start = time.time()
    yield  # everything before yield is __enter__, after is __exit__
    elapsed = time.time() - start
    print(f"{label}: {elapsed:.4f}s")

with timer("Data processing"):
    process_data()
```

### 11.4 Spark's Context Manager Pattern

```python
# SparkSession itself is not a context manager, but the pattern is common:
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
try:
    # ... processing ...
    pass
finally:
    spark.stop()
```

---

## 12. Python Packaging and Environments

### 12.1 Virtual Environments

Python's packaging ecosystem isolates project dependencies using virtual environments:

```bash
# Create a virtual environment
python -m venv myenv

# Activate (Linux/Mac)
source myenv/bin/activate

# Activate (Windows)
myenv\Scripts\activate

# Install packages
pip install pandas==2.0.0 pyspark==3.5.0

# Freeze dependencies
pip freeze > requirements.txt

# Reproduce environment elsewhere
pip install -r requirements.txt

# Deactivate
deactivate
```

### 12.2 `conda` Environments

Conda manages both Python packages and system-level dependencies (C libraries, etc.):

```bash
conda create -n spark_env python=3.10 pandas pyspark
conda activate spark_env
conda install -c conda-forge delta-spark
conda env export > environment.yml
conda env create -f environment.yml
```

### 12.3 `pip` vs `conda`

| Aspect | pip | conda |
|--------|-----|-------|
| Package source | PyPI | Anaconda repos + conda-forge |
| Language support | Python only | Any language (C, R, Java, etc.) |
| Dependency resolution | Limited (backtracking since pip 20.3) | Full SAT solver |
| Environment | venv | conda environments |
| Speed | Fast | Slower (comprehensive solver) |

### 12.4 Why This Matters for Data Engineering

On a Spark cluster, every executor needs the same Python environment. Mismatched versions cause cryptic errors:

```
# Common error when driver and executor have different library versions:
ModuleNotFoundError: No module named 'some_library'
# or
AttributeError: module 'pandas' has no attribute 'new_feature'
```

Solutions:
- **Databricks**: Use cluster libraries or `%pip install` in notebooks
- **YARN**: Ship a conda-pack archive as `--archives`
- **Kubernetes**: Build a custom Docker image with dependencies

---

## 13. Memory Profiling and Optimization

### 13.1 Measuring Memory Usage

```python
import sys
import tracemalloc

# sys.getsizeof: shallow size of a single object
sys.getsizeof([1, 2, 3])  # 120 bytes (the list object, not its contents)

# tracemalloc: track memory allocations
tracemalloc.start()
data = [i for i in range(1_000_000)]
snapshot = tracemalloc.take_snapshot()
top = snapshot.statistics("lineno")
for stat in top[:5]:
    print(stat)
```

### 13.2 `memory_profiler` -- Line-by-Line

```python
# pip install memory_profiler
from memory_profiler import profile

@profile
def process_data():
    data = [i for i in range(1_000_000)]   # +7.6 MB
    squares = [x**2 for x in data]         # +8.0 MB
    filtered = [x for x in squares if x > 100]  # +7.9 MB
    return sum(filtered)
```

Running `python -m memory_profiler script.py` shows memory consumption per line.

### 13.3 Optimization Techniques

**1. Use generators instead of lists**:
```python
# Bad: 80 MB
data = [x**2 for x in range(10_000_000)]
total = sum(data)

# Good: ~0 MB
total = sum(x**2 for x in range(10_000_000))
```

**2. Use `__slots__` for data classes with many instances**:
```python
class Record:
    __slots__ = ('id', 'name', 'value')
    def __init__(self, id, name, value):
        self.id = id
        self.name = name
        self.value = value
```

**3. Process data in chunks**:
```python
import pandas as pd

# Bad: load entire CSV into memory
df = pd.read_csv("huge.csv")

# Good: process in chunks
for chunk in pd.read_csv("huge.csv", chunksize=100_000):
    process(chunk)
```

**4. Use appropriate data types**:
```python
# Bad: float64 by default (8 bytes per value)
df = pd.read_csv("data.csv")  # 800 MB

# Good: downcast types
df = pd.read_csv("data.csv")
df["int_col"] = df["int_col"].astype("int32")       # 4 bytes instead of 8
df["float_col"] = df["float_col"].astype("float32")  # 4 bytes instead of 8
df["cat_col"] = df["cat_col"].astype("category")     # huge savings for low-cardinality
```

**5. Delete intermediate results**:
```python
raw_data = load_data()
processed = transform(raw_data)
del raw_data           # free memory
gc.collect()           # force garbage collection
final = aggregate(processed)
```

---

## 14. Common Interview Patterns

### 14.1 Mutable Default Arguments

```python
# BUG: The default list is shared across ALL calls
def append_to(element, target=[]):
    target.append(element)
    return target

print(append_to(1))  # [1]
print(append_to(2))  # [1, 2] -- unexpected! Same list object

# FIX: Use None as default
def append_to(element, target=None):
    if target is None:
        target = []
    target.append(element)
    return target
```

### 14.2 Shallow vs Deep Copy

```python
import copy

original = [[1, 2], [3, 4]]

# Shallow copy: new outer list, same inner lists
shallow = copy.copy(original)
shallow[0].append(99)
print(original)  # [[1, 2, 99], [3, 4]] -- inner list modified!

# Deep copy: new outer list, new inner lists (recursive)
original = [[1, 2], [3, 4]]
deep = copy.deepcopy(original)
deep[0].append(99)
print(original)  # [[1, 2], [3, 4]] -- unaffected
```

### 14.3 `is` vs `==`

```python
a = [1, 2, 3]
b = [1, 2, 3]

print(a == b)   # True -- same value
print(a is b)   # False -- different objects

a = None
print(a is None)    # True -- use 'is' for None/True/False (singletons)
print(a == None)    # True but bad practice (triggers __eq__)
```

### 14.4 List Comprehension Scope

```python
# In Python 3, list comprehension variables don't leak:
x = 10
result = [x for x in range(5)]
print(x)  # 10 -- 'x' in comprehension is scoped

# But in Python 2, they did leak:
# print(x)  # Would print 4
```

### 14.5 `*args` and `**kwargs`

```python
def flexible(*args, **kwargs):
    print(f"Positional: {args}")    # tuple
    print(f"Keyword: {kwargs}")      # dict

flexible(1, 2, 3, name="Alice", age=30)
# Positional: (1, 2, 3)
# Keyword: {'name': 'Alice', 'age': 30}

# Unpacking:
def add(a, b, c):
    return a + b + c

values = [1, 2, 3]
print(add(*values))  # 6

params = {"a": 1, "b": 2, "c": 3}
print(add(**params))  # 6
```

### 14.6 `@staticmethod` vs `@classmethod`

```python
class MyClass:
    class_var = 0

    @staticmethod
    def utility(x, y):
        return x + y  # no access to instance or class

    @classmethod
    def create(cls, data):
        instance = cls()  # 'cls' is the class itself
        instance.data = data
        cls.class_var += 1
        return instance
```

### 14.7 `__str__` vs `__repr__`

```python
class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"Point({self.x}, {self.y})"  # for developers, unambiguous

    def __str__(self):
        return f"({self.x}, {self.y})"  # for users, readable

p = Point(3, 4)
print(repr(p))  # Point(3, 4) -- used by debugger, REPL
print(str(p))   # (3, 4) -- used by print()
```

### 14.8 Comprehension Patterns

```python
# Dict comprehension
word_lengths = {word: len(word) for word in ["hello", "world"]}

# Set comprehension
unique_lengths = {len(word) for word in ["hello", "world", "hi"]}

# Nested comprehension
matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
flat = [x for row in matrix for x in row]  # [1, 2, 3, 4, 5, 6, 7, 8, 9]

# Conditional comprehension
evens = [x for x in range(20) if x % 2 == 0]
labels = ["even" if x % 2 == 0 else "odd" for x in range(10)]
```

### 14.9 Exception Handling Patterns

```python
# Specific exceptions first
try:
    result = data[key]
except KeyError:
    result = default_value
except (TypeError, ValueError) as e:
    log_error(e)
    result = None
except Exception as e:
    log_critical(e)
    raise  # re-raise after logging
else:
    process(result)  # runs ONLY if no exception
finally:
    cleanup()  # ALWAYS runs
```

### 14.10 Dataclasses (Python 3.7+)

```python
from dataclasses import dataclass, field
from typing import List

@dataclass
class PipelineConfig:
    name: str
    input_path: str
    output_path: str
    partitions: int = 200
    tags: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.partitions < 1:
            raise ValueError("partitions must be >= 1")

config = PipelineConfig("daily_agg", "/data/raw", "/data/processed")
print(config)  # PipelineConfig(name='daily_agg', input_path='/data/raw', ...)
```

### 14.11 Type Hints

```python
from typing import Dict, List, Optional, Tuple, Union

def process_records(
    records: List[Dict[str, Union[str, int]]],
    batch_size: int = 100,
    output_path: Optional[str] = None
) -> Tuple[int, int]:
    """Returns (success_count, failure_count)."""
    pass

# Python 3.10+ simplified syntax:
def process(records: list[dict[str, str | int]], path: str | None = None) -> tuple[int, int]:
    pass
```

---

## 15. Quick-Reference Cheat Sheet

### Memory & Performance

| Concept | Key Fact |
|---------|----------|
| Python int | 28 bytes (vs 4 bytes in C) |
| Empty dict | 64 bytes |
| Empty list | 56 bytes |
| `__slots__` | Saves 50-70% memory per instance |
| GIL | Only 1 thread executes bytecode at a time |
| GIL releases during | I/O, C extensions, sleep |
| NumPy vs loop | ~50-100x faster for numeric operations |
| Generator vs list | O(1) memory vs O(n) memory |

### Data Structure Complexity

| Operation | list | dict | set | deque |
|-----------|------|------|-----|-------|
| Index/lookup | O(1) | O(1) | O(1) | O(n) |
| Append/add | O(1)* | O(1)* | O(1)* | O(1) |
| Insert at 0 | O(n) | -- | -- | O(1) |
| Search | O(n) | O(1) | O(1) | O(n) |
| Delete | O(n) | O(1) | O(1) | O(n)/O(1) ends |

*amortized

### Concurrency Decision Tree

```
Is the workload CPU-bound?
├── YES → multiprocessing (or Spark for large data)
└── NO (I/O-bound)
    ├── Few concurrent operations → threading
    └── Thousands of connections → asyncio
```

### Serialization for Spark

| What | Serializer | Why |
|------|-----------|-----|
| UDF functions | cloudpickle | Handles lambdas and closures |
| Broadcast data | pickle | Standard Python objects |
| Pandas UDF data | Apache Arrow | Columnar, zero-copy, fast |
| Shuffle data | Java/Kryo | Stays in JVM, no Python involved |

### Interview Red Flags to Avoid

| Bad | Good |
|-----|------|
| "Python is slow" | "Python loops are slow; vectorized ops and C extensions are fast" |
| "Use threads for parallelism" | "Threads for I/O, processes for CPU, Spark for distributed" |
| "is and == are the same" | "is checks identity, == checks equality; use is for None" |
| "I always use lists" | "Lists for ordered, dicts for lookup, sets for membership, deques for queues" |
