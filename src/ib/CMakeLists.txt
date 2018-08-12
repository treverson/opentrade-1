file(GLOB_RECURSE SRC_FILES *.cc *.cpp)

add_library(ib MODULE
  ${SRC_FILES}
)
