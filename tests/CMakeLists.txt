set(TEST_SOURCES
    integration_test.cpp
    broker/broker_test.cpp
    broker/metrics_test.cpp
    cluster/metadata_test.cpp
    config/config_test.cpp
    config/properties_test.cpp
    logging/logger_test.cpp
    protocol/parser_test.cpp
    protocol/serializer_test.cpp
)

foreach(TEST_FILE ${TEST_SOURCES})
    get_filename_component(TEST_NAME ${TEST_FILE} NAME_WE)
    add_executable(${TEST_NAME} ${TEST_FILE})
    target_link_libraries(${TEST_NAME} PRIVATE TinyKafka_core GTest::gtest_main)
    set_target_properties(${TEST_NAME} PROPERTIES CXX_CLANG_TIDY "")
    if(TEST_NAME STREQUAL "integration_test")
        gtest_discover_tests(${TEST_NAME} PROPERTIES RUN_SERIAL TRUE)
    else()
        gtest_discover_tests(${TEST_NAME})
    endif()
endforeach()
