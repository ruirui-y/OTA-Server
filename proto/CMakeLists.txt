# 设备控制协议。服务端和客户端共用这一份定义。
set(DEVICE_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/device_control.proto)
set(DEVICE_PB_CC ${CMAKE_CURRENT_BINARY_DIR}/device_control.pb.cc)
set(DEVICE_PB_H ${CMAKE_CURRENT_BINARY_DIR}/device_control.pb.h)

add_custom_command(
    OUTPUT ${DEVICE_PB_CC} ${DEVICE_PB_H}
    COMMAND ${PROTOC_EXECUTABLE} --cpp_out=${CMAKE_CURRENT_BINARY_DIR} -I${CMAKE_CURRENT_SOURCE_DIR} ${DEVICE_PROTO}
    DEPENDS ${DEVICE_PROTO}
    COMMENT "protoc 生成 device_control.pb.cc"
)

add_library(ota_proto STATIC
    ${DEVICE_PB_CC}
    ${DEVICE_PB_H}
)
target_include_directories(ota_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(ota_proto PUBLIC protobuf::libprotobuf ota_build_options)

# protoc 生成的代码里有「未引用形参」的告警，不是我们写的。
if(NOT MSVC)
    target_compile_options(ota_proto PRIVATE -Wno-unused-parameter)
endif()