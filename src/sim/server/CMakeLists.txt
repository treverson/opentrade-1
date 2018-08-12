file(GLOB_RECURSE SRC_FILES *.cc)
add_library(sim_server MODULE ${SRC_FILES})
target_link_libraries(sim_server ${QUICKFIX_LIBRARY_PATH})
