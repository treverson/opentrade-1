file(GLOB_RECURSE SRC_FILES *.cc)
add_library(sim_client MODULE ${SRC_FILES})
target_link_libraries(sim_client ${QUICKFIX_LIBRARY_PATH})
