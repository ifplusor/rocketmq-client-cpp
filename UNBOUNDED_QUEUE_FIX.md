# Fix for unbounded_queue Data Loss Bug

## Problem Summary

The `unbounded_queue::pop_impl()` method had a fatal flaw that could cause data loss under high concurrency. When multiple threads competed to pop the last element from the queue, race conditions could occur leading to:
- Lost queue elements
- Queue corruption
- Incorrect empty queue detection

## Root Cause

The bug occurred in the single-element case handling (lines 117-137 of the original code):

```cpp
if (head_.compare_exchange_weak(head, nullptr)) {  // <-- BUG: Sets to nullptr!
  auto next = head->next.load();
  if (next == sentinel) {
    // ... handling for single element ...
  }
  head_.store(next);
  return head;
}
```

When processing the last element, the code temporarily set `head_` to `nullptr`. During this window:
1. Other threads could load `head_` as `nullptr`
2. Those threads would reload, potentially seeing an inconsistent state
3. Race conditions could cause elements to be skipped or processed multiple times

## Solution

The fix follows the approach used in the Go port (https://github.com/vanus-labs/vanus/blob/main/lib/container/conque/unbounded/queue.go):

### Key Changes:

1. **Added lock_sentinel**: A special sentinel value to indicate a locked state, distinct from `nullptr` and the empty sentinel
   ```cpp
   node_type* const lock_sentinel;
   ```

2. **Use lock_sentinel instead of nullptr**: When locking for single-element operations
   ```cpp
   if (!head_.compare_exchange_weak(head, lock_sentinel)) {
     continue;
   }
   ```

3. **Proper wait mechanism**: Threads that encounter `lock_sentinel` wait for it to be released
   ```cpp
   if (head == lock_sentinel) {
     head = wait_for_unlock_or_stable(head_, lock_sentinel);
   }
   ```

4. **Unified wait helper**: Efficient spinning with CPU pause instructions
   ```cpp
   template <typename AtomicPtr>
   node_type* wait_for_unlock_or_stable(AtomicPtr& atomic_ptr, node_type* unexpected) noexcept {
     // Active spin with CPU pause, then passive yield
   }
   ```

## Testing

Comprehensive tests were added to verify the fix:

- **BasicPushPop**: Verifies single-threaded operations
- **MultiplePushPop**: Verifies ordered queue operations
- **ConcurrentPushPop**: Multi-threaded producer/consumer test
- **ConcurrentSingleElementStress**: **Critical test** - 10,000 iterations with 4 competing consumers to verify no data loss in the exact scenario where the bug occurred

All tests pass successfully, confirming the fix eliminates the data loss issue.

## Performance Considerations

The fix introduces minimal overhead:
- Lock acquisition only occurs for single-element pop operations
- Uses efficient CPU pause instructions for spinning (x86, ARM)
- Wait logic is optimized with active then passive spinning
- No additional heap allocations (lock_sentinel is allocated once at construction)

## Compatibility

The fix maintains full API compatibility:
- No changes to public interfaces
- Same memory ordering guarantees
- Same exception guarantees (noexcept where applicable)

## References

- Original Go fix: https://github.com/vanus-labs/vanus/blob/main/lib/container/conque/unbounded/queue.go
- Issue discussion: https://github.com/ifplusor/rocketmq-client-cpp/blob/re_dev/src/concurrent/unbounded_queue.hpp
