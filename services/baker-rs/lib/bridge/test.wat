(module $test.wasm
  (type (;0;) (func))
  (type (;1;) (func (result i32)))
  (type (;2;) (func (param i64 i64) (result i64)))
  (type (;3;) (func (param i32)))
  (type (;4;) (func (param i64) (result i64)))
  (type (;5;) (func (param i64 i32 i32) (result i64)))
  (type (;6;) (func (param i32) (result i64)))
  (type (;7;) (func (param i64)))
  (import "env" "_copy_dataseries" (func $_copy_dataseries (type 4)))
  (import "env" "_set_interpolation_strategy" (func $_set_interpolation_strategy (type 2)))
  (import "env" "_set_time_window" (func $_set_time_window (type 2)))
  (import "env" "_add" (func $_add (type 2)))
  (import "env" "_mirroring" (func $_mirroring (type 5)))
  (import "env" "_get_dataseries_id_by_alias" (func $_get_dataseries_id_by_alias (type 6)))
  (import "env" "_set_result_dataseries" (func $_set_result_dataseries (type 7)))
  (import "wasi_snapshot_preview1" "proc_exit" (func $__wasi_proc_exit (type 3)))
  (func $__wasm_call_ctors (type 0)
    call $emscripten_stack_init)
  (func $compute (type 2) (param i64 i64) (result i64)
    (local i32 i32 i32 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i64 i32 i32 i64 i64 i32 i32)
    global.get $__stack_pointer
    local.set 2
    i32.const 64
    local.set 3
    local.get 2
    local.get 3
    i32.sub
    local.set 4
    local.get 4
    global.set $__stack_pointer
    local.get 4
    local.get 0
    i64.store offset=56
    local.get 4
    local.get 1
    i64.store offset=48
    local.get 4
    i64.load offset=56
    local.set 5
    local.get 5
    call $_copy_dataseries
    local.set 6
    local.get 4
    local.get 6
    i64.store offset=40
    local.get 4
    i64.load offset=40
    local.set 7
    i64.const 1
    local.set 8
    local.get 7
    local.get 8
    call $_set_interpolation_strategy
    local.set 9
    local.get 4
    local.get 9
    i64.store offset=40
    local.get 4
    i64.load offset=40
    local.set 10
    i64.const 817405952
    local.set 11
    local.get 10
    local.get 11
    call $_set_time_window
    local.set 12
    local.get 4
    local.get 12
    i64.store offset=40
    local.get 4
    i64.load offset=48
    local.set 13
    local.get 13
    call $_copy_dataseries
    local.set 14
    local.get 4
    local.get 14
    i64.store offset=32
    local.get 4
    i64.load offset=32
    local.set 15
    i64.const 1
    local.set 16
    local.get 15
    local.get 16
    call $_set_interpolation_strategy
    local.set 17
    local.get 4
    local.get 17
    i64.store offset=32
    local.get 4
    i64.load offset=32
    local.set 18
    i64.const 817405952
    local.set 19
    local.get 18
    local.get 19
    call $_set_time_window
    local.set 20
    local.get 4
    local.get 20
    i64.store offset=32
    local.get 4
    i64.load offset=40
    local.set 21
    local.get 4
    i64.load offset=32
    local.set 22
    local.get 21
    local.get 22
    call $_add
    local.set 23
    local.get 4
    local.get 23
    i64.store offset=24
    local.get 4
    i64.load offset=56
    local.set 24
    local.get 4
    local.get 24
    i64.store
    local.get 4
    i64.load offset=48
    local.set 25
    local.get 4
    local.get 25
    i64.store offset=8
    local.get 4
    i64.load offset=24
    local.set 26
    local.get 4
    local.set 27
    i32.const 2
    local.set 28
    local.get 26
    local.get 27
    local.get 28
    call $_mirroring
    local.set 29
    local.get 4
    local.get 29
    i64.store offset=24
    local.get 4
    i64.load offset=24
    local.set 30
    i32.const 64
    local.set 31
    local.get 4
    local.get 31
    i32.add
    local.set 32
    local.get 32
    global.set $__stack_pointer
    local.get 30
    return)
  (func $__original_main (type 1) (result i32)
    (local i32 i32 i32 i32 i32 i64 i64 i64 i32 i32 i32 i32 i32 i64 i64 i64 i32 i32 i32 i32 i64 i64 i64 i64 i32 i32 i32 i32)
    global.get $__stack_pointer
    local.set 0
    i32.const 32
    local.set 1
    local.get 0
    local.get 1
    i32.sub
    local.set 2
    local.get 2
    global.set $__stack_pointer
    i32.const 0
    local.set 3
    local.get 2
    local.get 3
    i32.store offset=28
    i32.const 65549
    local.set 4
    local.get 4
    call $_get_dataseries_id_by_alias
    local.set 5
    local.get 2
    local.get 5
    i64.store offset=16
    local.get 2
    i64.load offset=16
    local.set 6
    i64.const 0
    local.set 7
    local.get 6
    local.get 7
    i64.lt_s
    local.set 8
    i32.const 1
    local.set 9
    local.get 8
    local.get 9
    i32.and
    local.set 10
    block  ;; label = @1
      block  ;; label = @2
        local.get 10
        i32.eqz
        br_if 0 (;@2;)
        i32.const -1
        local.set 11
        local.get 2
        local.get 11
        i32.store offset=28
        br 1 (;@1;)
      end
      i32.const 65536
      local.set 12
      local.get 12
      call $_get_dataseries_id_by_alias
      local.set 13
      local.get 2
      local.get 13
      i64.store offset=8
      local.get 2
      i64.load offset=8
      local.set 14
      i64.const 0
      local.set 15
      local.get 14
      local.get 15
      i64.lt_s
      local.set 16
      i32.const 1
      local.set 17
      local.get 16
      local.get 17
      i32.and
      local.set 18
      block  ;; label = @2
        local.get 18
        i32.eqz
        br_if 0 (;@2;)
        i32.const -1
        local.set 19
        local.get 2
        local.get 19
        i32.store offset=28
        br 1 (;@1;)
      end
      local.get 2
      i64.load offset=16
      local.set 20
      local.get 2
      i64.load offset=8
      local.set 21
      local.get 20
      local.get 21
      call $compute
      local.set 22
      local.get 2
      local.get 22
      i64.store
      local.get 2
      i64.load
      local.set 23
      local.get 23
      call $_set_result_dataseries
      i32.const 0
      local.set 24
      local.get 2
      local.get 24
      i32.store offset=28
    end
    local.get 2
    i32.load offset=28
    local.set 25
    i32.const 32
    local.set 26
    local.get 2
    local.get 26
    i32.add
    local.set 27
    local.get 27
    global.set $__stack_pointer
    local.get 25
    return)
  (func $_start (type 0)
    block  ;; label = @1
      i32.const 1
      i32.eqz
      br_if 0 (;@1;)
      call $__wasm_call_ctors
    end
    call $__original_main
    call $exit
    unreachable)
  (func $dummy (type 0))
  (func $libc_exit_fini (type 0)
    (local i32)
    i32.const 0
    local.set 0
    block  ;; label = @1
      i32.const 0
      i32.const 0
      i32.le_u
      br_if 0 (;@1;)
      loop  ;; label = @2
        local.get 0
        i32.const -4
        i32.add
        local.tee 0
        i32.load
        call_indirect (type 0)
        local.get 0
        i32.const 0
        i32.gt_u
        br_if 0 (;@2;)
      end
    end
    call $dummy)
  (func $exit (type 3) (param i32)
    call $dummy
    call $libc_exit_fini
    call $dummy
    local.get 0
    call $_Exit
    unreachable)
  (func $_Exit (type 3) (param i32)
    local.get 0
    call $__wasi_proc_exit
    unreachable)
  (func $emscripten_stack_init (type 0)
    i32.const 65536
    global.set $__stack_base
    i32.const 0
    i32.const 15
    i32.add
    i32.const -16
    i32.and
    global.set $__stack_end)
  (func $emscripten_stack_get_free (type 1) (result i32)
    global.get $__stack_pointer
    global.get $__stack_end
    i32.sub)
  (func $emscripten_stack_get_base (type 1) (result i32)
    global.get $__stack_base)
  (func $emscripten_stack_get_end (type 1) (result i32)
    global.get $__stack_end)
  (func $_emscripten_stack_restore (type 3) (param i32)
    local.get 0
    global.set $__stack_pointer)
  (func $emscripten_stack_get_current (type 1) (result i32)
    global.get $__stack_pointer)
  (table (;0;) 2 2 funcref)
  (memory (;0;) 258 258)
  (global $__stack_pointer (mut i32) (i32.const 65536))
  (global $__stack_end (mut i32) (i32.const 0))
  (global $__stack_base (mut i32) (i32.const 0))
  (export "memory" (memory 0))
  (export "__indirect_function_table" (table 0))
  (export "_start" (func $_start))
  (export "emscripten_stack_init" (func $emscripten_stack_init))
  (export "emscripten_stack_get_free" (func $emscripten_stack_get_free))
  (export "emscripten_stack_get_base" (func $emscripten_stack_get_base))
  (export "emscripten_stack_get_end" (func $emscripten_stack_get_end))
  (export "_emscripten_stack_restore" (func $_emscripten_stack_restore))
  (export "emscripten_stack_get_current" (func $emscripten_stack_get_current))
  (elem (;0;) (i32.const 1) func $__wasm_call_ctors)
  (data $.rodata (i32.const 65536) "dataseries_b\00dataseries_a\00"))
