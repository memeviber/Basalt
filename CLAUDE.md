# CLAUDE.md — Basalt Bootstrap Compiler

## Phạm vi làm việc

Nguồn compiler đang được duy trì cho self-hosting là `src/bootstrap/basaltc.basalt`. Mọi thay đổi compiler, probe và regression mới phải dogfood qua Bootstrap compiler. **Không sửa, không build và không dùng `src/compiler/`** trong các vòng audit Bootstrap.

`src/bootstrap/basaltc.seed.c` là frozen compiler seed dùng để dựng thế hệ Bootstrap hiện tại. File `src/bootstrap/fixed_point_production.sha256` phải khớp SHA-256 thực tế của seed trước khi coi fixed-point là hợp lệ.

## Kiến trúc pipeline

Pipeline chuẩn là:

```text
basaltc.seed.c
  → seed binary
  → src/bootstrap/basaltc.basalt
  → stage2.c / stage2 binary
  → current.c / current binary
  → generated C11 program
  → GCC hoặc Clang binary
```

`src/bootstrap/basaltc.basalt` chứa lexer, parser/AST, type checker, ownership/borrow checker, generic specialization, C emitter, include expansion và CLI. Runtime C được emitter nhúng vào generated program; vì vậy generated C có thể lớn hơn phần application AST.

## Lệnh kiểm chứng chuẩn

Từ root repository, dùng các lệnh sau:

```bash
bash scripts/run_regression.sh
bash scripts/run_ownership_stress.sh
bash scripts/run_ffi_portability.sh
bash scripts/fixed_point.sh
```

Cờ strict mặc định là:

```text
-std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror
```

Các fixture hợp lệ phải compile và chạy thành công. Fixture không hợp lệ phải bị Bootstrap compiler từ chối với diagnostic code đúng. Generated C phải được kiểm tra thêm bằng ASan/UBSan khi fixture có ownership, pointer, string, container, FFI hoặc control-flow cleanup.

## Quy tắc audit bug

Không ghi nhận một nghi vấn là compiler bug chỉ dựa trên generated C nhìn xấu. Trước tiên phải tạo reproduction tối thiểu, chạy qua Bootstrap compiler fresh, kiểm tra generated C, compile strict GCC, chạy runtime và nếu liên quan memory/ownership thì chạy ASan/UBSan. Sau đó phân loại thành:

| Nhóm | Tiêu chí |
|---|---|
| Compiler bug | Source hợp lệ nhưng generated C sai, semantics sai hoặc diagnostic sai |
| Invalid syntax | Parser phải từ chối theo grammar hiện tại |
| Conservative policy | Type checker từ chối an toàn dù C có thể chạy |
| Feature gap | Tính năng chưa được ngôn ngữ định nghĩa |
| Runtime safety abort | Source được compile nhưng runtime registry/panic chủ động dừng vì vi phạm ownership/runtime contract |

Không sửa policy bảo thủ thành permissive chỉ để một probe pass. Mọi thay đổi phải giữ fixed-point, strict portability và sanitizer behavior.

## Control-flow và defer

`for` được sinh trực tiếp thành C `for`; step nằm trong phần increment của C. Không clone hoặc sinh step thủ công trước `continue`, vì C tự chạy step sau `continue`.

`while` và `for` yêu cầu body dạng block ở parser. `if` không được nhận direct `defer`, direct local declaration hoặc direct tuple binding làm branch body; các trường hợp này lần lượt dùng diagnostic code 77 và 78 để tránh generated C kiểu `if (...) declaration;` hoặc `if (...) defer;` không hợp lệ.

`match` tạo temporary subject và chuỗi `if/else if/else` trên tag. Mỗi arm có block C và cleanup scope độc lập. Wildcard/default phải xuất hiện một lần và ở cuối; nếu không có default thì các variant phải exhaustive.

`defer` được lưu trong emitter stack. Block/match arm lưu mốc stack, flush cleanup theo thứ tự ngược trước khi scope đóng, và control-flow exit flush các defer thuộc scope bị thoát. `return expr` phải materialize expression trước cleanup để defer không làm thay đổi giá trị trả về.

Generated C có thể còn các statement unreachable sau `return`, `break` hoặc `continue` trong các path khác nhau. Chúng không được coi là lỗi nếu strict C, runtime và sanitizer đều pass; emitter hiện loại bỏ các cleanup unreachable mà termination analysis chứng minh được.

## Ownership và borrow checking

Type checker dùng flow snapshots cho `if`, `while`, `for` và `match`. Match arms được phân tích từ baseline rồi join state; move ở một arm không được lan trực tiếp thành false positive sang arm loại trừ khác. Chính sách hiện tại cố ý bảo thủ với root-place borrow; raw pointer, pointer arithmetic, `extern` và `includec` là ranh giới low-level phải kiểm tra bằng sanitizer.

Một edge case cần giữ trong mind khi audit: deferred release như `defer str::free(value);` được thực thi ở cleanup point. Nếu chương trình cũng release cùng value bằng một statement khác, runtime registry có thể panic code 2. Đây là runtime safety behavior hiện tại, không tự động coi là compiler miscompile nếu chưa có đặc tả yêu cầu compile-time rejection.

## Quy tắc thay đổi file

Chỉ stage source/harness/regression fixture có chủ đích. Không commit `.tmp/`, generated C, executable, benchmark output, compiler log hoặc audit notes bị ignore. Trước commit cần chạy:

```bash
git diff --check
git status --short
```

Sau source change, phải dựng Bootstrap stage fresh và chạy lại focused tests trước khi chạy full gates. Nếu source cuối thay đổi code emitter/type checker, phải chạy fixed-point lại trước seed promotion; seed promotion chỉ được thực hiện khi `n3.c == n4.c` và checksum được cập nhật đúng.

## Đồ thị mã nguồn

Đồ thị cấp cao của pipeline và các vùng control-flow/defer được lưu tại `.tmp/bootstrap-source-graph.mmd`. PNG render tương ứng là `.tmp/bootstrap-source-graph.png`. Đây là đồ thị quan hệ kiến trúc, không thay thế call graph đầy đủ của toàn bộ 8,000+ dòng Bootstrap source.
