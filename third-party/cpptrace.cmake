include(FetchContent)
FetchContent_Declare(
  cpptrace
  GIT_REPOSITORY https://github.com/jeremy-rifkin/cpptrace.git
  GIT_TAG        v0.8.3 # <HASH or TAG>
  # SOURCE_DIR     ${CMAKE_CURRENT_SOURCE_DIR}/cpptrace
)
FetchContent_MakeAvailable(cpptrace)