#!/usr/bin/env bash

set -euo pipefail

BUILD_TYPE="Release"
BUILD_TEST=true
WITH_FUSE_OPT=false
WITH_ZK_INIT=false
WITH_RDMA=false
WITH_PROMETHEUS=false
WITH_OBS_STORAGE=false
WITH_ASAN=false
COVERAGE=false
RUN_LOCAL_SERVICE_FOR_COVERAGE=false
COMM_PLUGIN="brpc"
SERVICE_COVERAGE_GCOV_PREFIX="${SERVICE_COVERAGE_GCOV_PREFIX:-/tmp/falconfs_service_gcov}"

FALCONFS_INSTALL_DIR="${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}"
export FALCONFS_INSTALL_DIR=$FALCONFS_INSTALL_DIR
export PATH=$FALCONFS_INSTALL_DIR/bin:$FALCONFS_INSTALL_DIR/python/bin:${PATH:-}
export LD_LIBRARY_PATH=$FALCONFS_INSTALL_DIR/lib64:$FALCONFS_INSTALL_DIR/lib:$FALCONFS_INSTALL_DIR/python/lib:${LD_LIBRARY_PATH:-}

# Default command is build
COMMAND=${1:-build}

# Get source directory
FALCONFS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
POSTGRES_INCLUDE_DIR="$(pg_config --includedir)"
POSTGRES_LIB_DIR="$(pg_config --libdir)"
PG_PKGLIBDIR="$(pg_config --pkglibdir)"
export CONFIG_FILE="$FALCONFS_DIR/config/config.json"

# Set build directory
BUILD_DIR="${BUILD_DIR:-$FALCONFS_DIR/build}"

# Set default install directory
FALCON_META_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_meta"
FALCON_CM_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_cm"
FALCON_CN_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_cn"
FALCON_DN_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_dn"
FALCON_STORE_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_store"
FALCON_REGRESS_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_regress"
FALCON_CLIENT_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_client"
PYTHON_SDK_INSTALL_DIR="$FALCONFS_INSTALL_DIR/falcon_python_interface"
PRIVATE_DIRECTORY_TEST_INSTALL_DIR="$FALCONFS_INSTALL_DIR/private-directory-test"

set_comm_plugin() {
	local plugin="${1,,}"
	case "$plugin" in
	brpc | hcom)
		COMM_PLUGIN="$plugin"
		;;
	*)
		echo "Error: Unknown communication plugin '$1' (choose brpc|hcom)" >&2
		exit 1
		;;
	esac
}

parse_comm_plugin_option() {
	local args=("$@")
	local count=${#args[@]}
	for ((i = 0; i < count; i++)); do
		case "${args[i]}" in
		--comm-plugin=*)
			set_comm_plugin "${args[i]#*=}"
			;;
		--comm-plugin)
			if ((i + 1 < count)); then
				set_comm_plugin "${args[i + 1]}"
			else
				echo "Error: --comm-plugin requires a value (brpc|hcom)" >&2
				exit 1
			fi
			;;
		esac
	done
}

parse_comm_plugin_option "$@"

gen_proto() {
	mkdir -p "$BUILD_DIR"
	echo "Generating Protobuf files..."
	protoc --cpp_out="$BUILD_DIR" \
		--proto_path="$FALCONFS_DIR/remote_connection_def/proto" \
		falcon_meta_rpc.proto brpc_io.proto
	echo "Protobuf files generated."
}

build_comm_plugin() {
	case "$COMM_PLUGIN" in
	brpc)
		echo "Building brpc communication plugin..."
		cd "$FALCONFS_DIR/falcon" && make -f MakefilePlugin.brpc
		echo "brpc communication plugin build complete."
		;;
	hcom)
		echo "Building hcom communication plugin..."
		cd "$FALCONFS_DIR/falcon" && make -f MakefilePlugin.hcom WITH_OBS_STORAGE=$WITH_OBS_STORAGE
		echo "hcom communication plugin build complete."

		# Copy test plugins to plugins directory for hcom
		local test_plugin_src="$BUILD_DIR/test_plugins"
		local plugins_dest="$FALCONFS_DIR/plugins"
		if [[ -d "$test_plugin_src" ]]; then
			echo "Copying test plugins to $plugins_dest..."
			mkdir -p "$plugins_dest"
			cp -f "$test_plugin_src"/*.so "$plugins_dest/" 2>/dev/null || true
			echo "Test plugins copied."
		fi
		;;
	esac
}

# build_falconfs
build_falconfs() {
	gen_proto

	PG_CFLAGS=""
	local cmake_coverage_extra_flags=""
	local cmake_coverage="OFF"
	if [[ "$BUILD_TYPE" == "Debug" ]]; then
		CONFIGURE_OPTS+=(--enable-debug)
		PG_CFLAGS="-ggdb -O0 -g3 -Wall -fno-omit-frame-pointer"
	else
		PG_CFLAGS="-O0 -g"
	fi
	if [[ "$COVERAGE" == true ]]; then
		cmake_coverage="ON"
		PG_CFLAGS="$PG_CFLAGS --coverage -fprofile-update=atomic"
		cmake_coverage_extra_flags="-fprofile-update=atomic"
	fi
	echo "Building FalconFS Meta (mode: $BUILD_TYPE)..."
	cd $FALCONFS_DIR/falcon
	ASAN_MAKE_OPT=""
	if [[ "$WITH_ASAN" == "true" ]]; then
		ASAN_MAKE_OPT="WITH_ASAN=1"
	fi
	make USE_PGXS=1 CFLAGS="-Wno-shadow $PG_CFLAGS" CXXFLAGS="-Wno-shadow $PG_CFLAGS" \
		FALCONFS_INSTALL_DIR="$FALCONFS_INSTALL_DIR" $ASAN_MAKE_OPT

	echo "Building FalconFS Client (mode: $BUILD_TYPE)..."
	cmake -B "$BUILD_DIR" -GNinja "$FALCONFS_DIR" \
		-DCMAKE_INSTALL_PREFIX=$FALCON_CLIENT_INSTALL_DIR \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
		-DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
		-DCMAKE_C_FLAGS="$cmake_coverage_extra_flags" \
		-DCMAKE_CXX_FLAGS="$cmake_coverage_extra_flags" \
		-DPOSTGRES_INCLUDE_DIR="$POSTGRES_INCLUDE_DIR" \
		-DPOSTGRES_LIB_DIR="$POSTGRES_LIB_DIR" \
		-DPG_PKGLIBDIR="$PG_PKGLIBDIR" \
		-DWITH_FUSE_OPT="$WITH_FUSE_OPT" \
		-DWITH_ZK_INIT="$WITH_ZK_INIT" \
		-DWITH_RDMA="$WITH_RDMA" \
		-DWITH_PROMETHEUS="$WITH_PROMETHEUS" \
		-DWITH_OBS_STORAGE="$WITH_OBS_STORAGE" \
		-DENABLE_COVERAGE="$cmake_coverage" \
		-DENABLE_ASAN="$WITH_ASAN" \
		-DBUILD_TEST=$BUILD_TEST &&
		cd "$BUILD_DIR" && ninja

	build_comm_plugin

	echo "FalconFS build complete."
}

# clean_falconfs
clean_falconfs() {
	echo "Cleaning FalconFS Meta"
	cd $FALCONFS_DIR/falcon
	make USE_PGXS=1 clean
	rm -rf $FALCONFS_DIR/falcon/connection_pool/fbs
	rm -rf $FALCONFS_DIR/falcon/brpc_comm_adapter/proto
	make -f MakefilePlugin.brpc clean || true
	make -f MakefilePlugin.hcom clean || true

	echo "Cleaning FalconFS Client..."
	rm -rf "$BUILD_DIR"
	echo "FalconFS clean complete."
}

clean_tests() {
	echo "Cleaning FalconFS tests..."
	rm -rf "$BUILD_DIR/tests"
	echo "FalconFS tests clean complete."
}

clean_coverage_data() {
	echo "Cleaning coverage artifacts..."
	find "$BUILD_DIR" "$FALCONFS_DIR/falcon" -type f \( -name "*.gcda" -o -name "*.gcno" -o -name "*.info" \) -delete 2>/dev/null || true
	rm -rf "$BUILD_DIR/coverage"
	rm -rf "$SERVICE_COVERAGE_GCOV_PREFIX"
	echo "Coverage artifacts cleaned."
}

require_coverage_tools() {
	for tool in lcov genhtml; do
		if ! command -v "$tool" >/dev/null 2>&1; then
			echo "Error: required tool '$tool' not found in PATH" >&2
			exit 1
		fi
	done
}

resolve_gcov_tool() {
	local compiler_major
	compiler_major="$(g++ -dumpversion | cut -d. -f1)"
	if command -v "gcov-$compiler_major" >/dev/null 2>&1; then
		echo "gcov-$compiler_major"
	elif command -v gcov >/dev/null 2>&1; then
		echo "gcov"
	else
		echo "Error: gcov tool not found in PATH" >&2
		exit 1
	fi
}

run_non_service_unit_tests() {
	cd "$FALCONFS_DIR"

	TARGET_DIRS=("$FALCONFS_DIR/build/tests/falcon_store/" "$FALCONFS_DIR/build/tests/falcon_plugin/" "$FALCONFS_DIR/build/tests/private-directory-test/")

	for TARGET_DIR in "${TARGET_DIRS[@]}"; do
		if [ -d "$TARGET_DIR" ]; then
			echo "Running tests in: $TARGET_DIR"
			find "$TARGET_DIR" -type f -executable -name "*UT" | while read -r executable_file; do
				if [[ "$(basename "$executable_file")" == "LocalRunWorkloadUT" ]]; then
					continue
				fi
				echo "Executing: $executable_file"
				"$executable_file"
				echo "---------------------------------------------------------------------------------------"
			done
		else
			echo "Test directory not found: $TARGET_DIR"
		fi
	done
	TARGET_DIR="$FALCONFS_DIR/build/tests/falcon/"
	find "$TARGET_DIR" -maxdepth 1 -type f -executable -not -name "*.cmake" -not -path "*/CMakeFiles/*" | while read -r executable_file; do
		echo "Executing: $executable_file"
		"$executable_file"
		echo "---------------------------------------------------------------------------------------"
	done
}

run_service_dependent_unit_tests() {
	local local_run_ut="$FALCONFS_DIR/build/tests/private-directory-test/LocalRunWorkloadUT"
	local service_server_ip
	local service_server_port
	service_server_ip="$(resolve_service_test_server_ip)"
	service_server_port="$(resolve_service_test_server_port)"
	if [[ -x "$local_run_ut" ]]; then
		echo "Running service-dependent tests in: $FALCONFS_DIR/build/tests/private-directory-test/"
		echo "Service-dependent UT endpoint: ${service_server_ip}:${service_server_port}"
		echo "Executing: $local_run_ut"
		SERVER_IP="$service_server_ip" \
		SERVER_PORT="$service_server_port" \
		LOCAL_RUN_MOUNT_DIR="${LOCAL_RUN_MOUNT_DIR:-/}" \
		LOCAL_RUN_FILE_PER_THREAD="${LOCAL_RUN_FILE_PER_THREAD:-1}" \
		LOCAL_RUN_THREAD_NUM_PER_CLIENT="${LOCAL_RUN_THREAD_NUM_PER_CLIENT:-1}" \
		LOCAL_RUN_CLIENT_ID="${LOCAL_RUN_CLIENT_ID:-0}" \
		LOCAL_RUN_MOUNT_PER_CLIENT="${LOCAL_RUN_MOUNT_PER_CLIENT:-1}" \
		LOCAL_RUN_CLIENT_CACHE_SIZE="${LOCAL_RUN_CLIENT_CACHE_SIZE:-16384}" \
		LOCAL_RUN_WAIT_PORT="${LOCAL_RUN_WAIT_PORT:-1111}" \
		LOCAL_RUN_FILE_SIZE="${LOCAL_RUN_FILE_SIZE:-4096}" \
		LOCAL_RUN_CLIENT_NUM="${LOCAL_RUN_CLIENT_NUM:-1}" \
		"$local_run_ut"
		echo "---------------------------------------------------------------------------------------"
	fi
}

run_unit_tests() {
	run_non_service_unit_tests
	run_service_dependent_unit_tests
	echo "All unit tests passed."
}

start_local_service_for_coverage() {
	echo "Starting local FalconFS service for service-dependent coverage tests..."
	rm -rf "$SERVICE_COVERAGE_GCOV_PREFIX"
	mkdir -p "$SERVICE_COVERAGE_GCOV_PREFIX"
	bash -lc "export GCOV_PREFIX='$SERVICE_COVERAGE_GCOV_PREFIX'; export GCOV_PREFIX_STRIP='0'; source '$FALCONFS_DIR/deploy/falcon_env.sh' && '$FALCONFS_DIR/deploy/falcon_start.sh'"
}

stop_local_service_for_coverage() {
	echo "Stopping local FalconFS service..."
	bash -lc "source '$FALCONFS_DIR/deploy/falcon_env.sh' && '$FALCONFS_DIR/deploy/falcon_stop.sh'"
}

resolve_service_test_server_ip() {
	if [[ -n "${LOCAL_RUN_META_SERVER_IP:-}" ]]; then
		echo "$LOCAL_RUN_META_SERVER_IP"
		return 0
	fi

	local meta_config="$FALCONFS_DIR/deploy/meta/falcon_meta_config.sh"
	if [[ -f "$meta_config" ]]; then
		local cn_ip=""
		cn_ip="$(bash -lc "source '$meta_config' >/dev/null 2>&1; printf '%s' \"\${cnIp:-}\"")"
		if [[ -n "$cn_ip" ]]; then
			echo "$cn_ip"
			return 0
		fi
	fi

	echo "127.0.0.1"
}

resolve_service_test_server_port() {
	if [[ -n "${LOCAL_RUN_META_SERVER_PORT:-}" ]]; then
		echo "$LOCAL_RUN_META_SERVER_PORT"
		return 0
	fi

	local meta_config="$FALCONFS_DIR/deploy/meta/falcon_meta_config.sh"
	if [[ -f "$meta_config" ]]; then
		local cn_port_prefix=""
		cn_port_prefix="$(bash -lc "source '$meta_config' >/dev/null 2>&1; printf '%s' \"\${cnPortPrefix:-}\"")"
		if [[ -n "$cn_port_prefix" ]]; then
			echo "${cn_port_prefix}0"
			return 0
		fi
	fi

	echo "55500"
}

generate_coverage_report() {
	require_coverage_tools
	local gcov_tool
	gcov_tool="$(resolve_gcov_tool)"
	local coverage_dir="$BUILD_DIR/coverage"
	local baseline_info="$coverage_dir/baseline.info"
	local raw_info="$coverage_dir/raw.info"
	local merged_info="$coverage_dir/merged.info"
	local filtered_info="$coverage_dir/filtered.info"
	local html_dir="$coverage_dir/html"

	mkdir -p "$coverage_dir"
	lcov --capture --initial --directory "$BUILD_DIR" --gcov-tool "$gcov_tool" --ignore-errors mismatch --output-file "$baseline_info"
	lcov --capture --directory "$BUILD_DIR" --gcov-tool "$gcov_tool" --ignore-errors mismatch --output-file "$raw_info"
	lcov -a "$baseline_info" -a "$raw_info" --output-file "$merged_info"
	lcov --remove "$merged_info" --ignore-errors unused '/usr/*' '*/third_party/*' '*/tests/*' '*/build/*_deps/*' '*/CMakeFiles/*' '*/build/generated/*' '*/generated/*' '*.pb.cc' '*.pb.h' '*.pb.c' '*.pb.hpp' '*.pb' '*.fbs.h' '*/brpc_comm_adapter/proto/*' '*/connection_pool/fbs/*' --output-file "$filtered_info"
	genhtml "$filtered_info" --output-directory "$html_dir" --title "FalconFS Coverage"
	echo "Coverage report generated: $html_dir/index.html"
}

run_coverage() {
	BUILD_TYPE="Debug"
	COVERAGE=true
	clean_coverage_data
	clean_falconfs
	build_falconfs
	local local_service_started=false
	cleanup_coverage_local_service() {
		if [[ "$local_service_started" == true ]]; then
			stop_local_service_for_coverage
			local_service_started=false
		fi
	}
	trap cleanup_coverage_local_service RETURN

	if [[ "$RUN_LOCAL_SERVICE_FOR_COVERAGE" == true ]]; then
		start_local_service_for_coverage
		local_service_started=true
		run_unit_tests
	else
		run_unit_tests
	fi

	if [[ "$local_service_started" == true ]]; then
		stop_local_service_for_coverage
		local_service_started=false
	fi
	generate_coverage_report
}

install_falcon_meta() {
	echo "Installing FalconFS meta ..."
	cd "$FALCONFS_DIR/falcon" && make USE_PGXS=1 install-falconfs \
		FALCONFS_INSTALL_DIR="$FALCONFS_INSTALL_DIR"
	echo "FalconFS meta installed"

	local plugin_src=""
	case "$COMM_PLUGIN" in
	brpc)
		plugin_src="$FALCONFS_DIR/falcon/libbrpcplugin.so"
		;;
	hcom)
		plugin_src="$FALCONFS_DIR/falcon/libhcomplugin.so"
		;;
	esac

    if [[ ! -f "$plugin_src" ]]; then
        echo "Error: communication plugin ($COMM_PLUGIN) not built at $plugin_src" >&2
        exit 1
    fi
    echo "copy ${COMM_PLUGIN} communication plugin to $FALCON_META_INSTALL_DIR/lib/postgresql..."
    cp "$plugin_src" "$FALCON_META_INSTALL_DIR/lib/postgresql/"
    echo "${COMM_PLUGIN} communication plugin copied."

	# 安装测试插件 (如果存在)
	if [[ -f "$FALCONFS_DIR/falcon/libfalcon_meta_service_test_plugin.so" ]]; then
		cp "$FALCONFS_DIR/falcon/libfalcon_meta_service_test_plugin.so" \
			"$FALCON_META_INSTALL_DIR/lib/postgresql/"
		echo "test plugin copied."
	fi
}

install_falcon_client() {
	echo "Installing FalconFS client to $FALCON_CLIENT_INSTALL_DIR..."

	cd "$BUILD_DIR" && ninja install

	# 复制配置文件
	mkdir -p "$FALCON_CLIENT_INSTALL_DIR/config"
	cp -r "$FALCONFS_DIR/config"/* "$FALCON_CLIENT_INSTALL_DIR/config/"
}

install_falcon_python_sdk() {
	echo "Installing FalconFS python sdk to $PYTHON_SDK_INSTALL_DIR..."
	rm -rf "$PYTHON_SDK_INSTALL_DIR"
	mkdir -p "$PYTHON_SDK_INSTALL_DIR"

	# 复制 python_interface 目录内容，排除 _pyfalconfs_internal
	for item in "$FALCONFS_DIR/python_interface"/*; do
		base=$(basename "$item")
		if [[ "$base" != "_pyfalconfs_internal" ]]; then
			cp -r "$item" "$PYTHON_SDK_INSTALL_DIR/"
		fi
	done

	echo "FalconFS python sdk installed to $PYTHON_SDK_INSTALL_DIR"
}

install_falcon_cm() {
	echo "Installing FalconFS cluster management scripts..."
	rm -rf "$FALCON_CM_INSTALL_DIR"
	mkdir -p "$FALCON_CM_INSTALL_DIR"

	# 从 cloud_native/falcon_cm/ 复制集群管理脚本
	cp -r "$FALCONFS_DIR/cloud_native/falcon_cm"/* "$FALCON_CM_INSTALL_DIR/"

	echo "FalconFS cluster management scripts installed"
}

install_falcon_cn() {
	echo "Installing FalconFS CN scripts..."
	rm -rf "$FALCON_CN_INSTALL_DIR"
	mkdir -p "$FALCON_CN_INSTALL_DIR"

	# 从 cloud_native/docker_build/cn/ 复制，排除 Dockerfile
	for file in "$FALCONFS_DIR/cloud_native/docker_build/cn"/*; do
		if [[ "$(basename "$file")" != "Dockerfile" ]]; then
			cp -r "$file" "$FALCON_CN_INSTALL_DIR/"
		fi
	done

	echo "FalconFS CN scripts installed"
}

install_falcon_dn() {
	echo "Installing FalconFS DN scripts..."
	rm -rf "$FALCON_DN_INSTALL_DIR"
	mkdir -p "$FALCON_DN_INSTALL_DIR"

	# 从 cloud_native/docker_build/dn/ 复制，排除 Dockerfile
	for file in "$FALCONFS_DIR/cloud_native/docker_build/dn"/*; do
		if [[ "$(basename "$file")" != "Dockerfile" ]]; then
			cp -r "$file" "$FALCON_DN_INSTALL_DIR/"
		fi
	done

	echo "FalconFS DN scripts installed"
}

install_falcon_store() {
	echo "Installing FalconFS Store scripts..."
	rm -rf "$FALCON_STORE_INSTALL_DIR"
	mkdir -p "$FALCON_STORE_INSTALL_DIR"

	# 只复制脚本文件，排除 falconfs 子目录和 Dockerfile
	for file in "$FALCONFS_DIR/cloud_native/docker_build/store"/*; do
		base=$(basename "$file")
		if [[ "$base" != "Dockerfile" && "$base" != "falconfs" ]]; then
			cp -r "$file" "$FALCON_STORE_INSTALL_DIR/"
		fi
	done

	echo "FalconFS Store scripts installed"
}

install_falcon_regress() {
	echo "Installing FalconFS Regress scripts..."
	rm -rf "$FALCON_REGRESS_INSTALL_DIR"
	mkdir -p "$FALCON_REGRESS_INSTALL_DIR"

	# 复制 regress 脚本
	for file in "$FALCONFS_DIR/cloud_native/docker_build/regress"/*; do
		base=$(basename "$file")
		if [[ "$base" != "Dockerfile" ]]; then
			cp -r "$file" "$FALCON_REGRESS_INSTALL_DIR/"
		fi
	done

	echo "FalconFS Regress scripts installed"
}

install_private_directory_test() {
	echo "Installing private-directory-test..."

	# 创建目录结构
	rm -rf "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR"
	mkdir -p "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin"
	mkdir -p "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/lib"

	# 复制可执行文件
	if [[ -f "$FALCONFS_DIR/build/tests/private-directory-test/test_falcon" ]]; then
		cp "$FALCONFS_DIR/build/tests/private-directory-test/test_falcon" \
			"$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/"
	fi
	if [[ -f "$FALCONFS_DIR/build/tests/private-directory-test/test_posix" ]]; then
		cp "$FALCONFS_DIR/build/tests/private-directory-test/test_posix" \
			"$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/"
	fi

	# 复制 FalconCMIT
	if [[ -f "$FALCONFS_DIR/build/tests/common/FalconCMIT" ]]; then
		cp "$FALCONFS_DIR/build/tests/common/FalconCMIT" \
			"$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/bin/"
	fi

    # 复制脚本文件（排除 C++ 源码）
    for file in "$FALCONFS_DIR/tests/private-directory-test"/*; do
        base=$(basename "$file")
        case "$base" in
            *.sh|*.py)
                cp "$file" "$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/"
                ;;
        esac
    done

	# 复制 README（如果存在）
	if [[ -f "$FALCONFS_DIR/tests/private-directory-test/README.md" ]]; then
		cp "$FALCONFS_DIR/tests/private-directory-test/README.md" \
			"$PRIVATE_DIRECTORY_TEST_INSTALL_DIR/"
	fi

	echo "private-directory-test installed"
}

install_deploy_scripts() {
	echo "Installing deploy scripts to $FALCONFS_INSTALL_DIR/deploy..."
	rm -rf "$FALCONFS_INSTALL_DIR/deploy"
	mkdir -p "$FALCONFS_INSTALL_DIR/deploy"
	# 复制 deploy 目录内容，排除 tmp 目录
	rsync -av --exclude='tmp' "$FALCONFS_DIR/deploy/" "$FALCONFS_INSTALL_DIR/deploy/"
	echo "deploy scripts installed to $FALCONFS_INSTALL_DIR/deploy"
}

print_help() {
	case "$1" in
	build)
		echo "Usage: $0 build [subcommand] [options]"
		echo ""
		echo "Build All Components of FalconFS"
		echo ""
		echo "Subcommands:"
		echo "  falcon           Build only FalconFS"
		echo ""
		echo "Options:"
		echo "  --debug              Build debug versions"
		echo "  --release            Build release versions (default)"
		echo "  --coverage           Build with gcov/lcov instrumentation"
		echo "  --comm-plugin=PLUGIN Communication plugin: brpc (default) or hcom"
		echo "  -h, --help           Show this help message"
		echo ""
		echo "Examples:"
		echo "  $0 build --debug                 # Build everything in debug mode"
		echo "  $0 build --debug --coverage      # Build with coverage instrumentation"
		echo "  $0 build --comm-plugin=hcom      # Build with hcom communication plugin"
		;;
	clean)
		echo "Usage: $0 clean [target] [options]"
		echo ""
		echo "Clean build artifacts and installations"
		echo ""
		echo "Targets:"
		echo "  falcon   Clean FalconFS build artifacts"
		echo "  test     Clean test binaries"
		echo "  coverage Clean coverage artifacts and report"
		echo ""
		echo "Options:"
		echo "  -h, --help  Show this help message"
		echo ""
		echo "Examples:"
		echo "  $0 clean           # Clean everything"
		echo "  $0 clean falcon    # Clean only FalconFS"
		;;
	coverage)
		echo "Usage: $0 coverage [options]"
		echo ""
		echo "Build FalconFS with coverage, run unit tests, and generate lcov html report"
		echo ""
		echo "Options:"
		echo "  --local-run        Start local service and run service-dependent UT cases"
		echo "  -h, --help         Show this help message"
		echo ""
		echo "Examples:"
		echo "  $0 coverage"
		echo "  $0 coverage --local-run"
		echo ""
		echo "Behavior:"
		echo "  $0 coverage             # do not start local service"
		echo "  $0 coverage --local-run # start local service"
		;;
	*)
		# General help information
		echo "Usage: $0 <command> [subcommand] [options]"
		echo ""
		echo "Commands:"
		echo "  build     Build components"
		echo "  clean     Clean artifacts"
		echo "  test      Run tests"
		echo "  coverage  Build, test and generate lcov report"
		echo "  install   Install components"
		echo ""
		echo "Run '$0 <command> --help' for more information on a specific command"
		;;
	esac
}

# Dispatch commands
case "$COMMAND" in
build)
	# Process shared build options (only debug/deploy allowed for combined build)
	while [[ $# -ge 2 ]]; do
		case "$2" in
		--debug)
			BUILD_TYPE="Debug"
			shift
			;;
		--deploy | --release)
			BUILD_TYPE="Release"
			shift
			;;
		--coverage)
			COVERAGE=true
			shift
			;;
		--help | -h)
			print_help "build"
			exit 0
			;;
		--comm-plugin)
			if [[ -z "${3:-}" ]]; then
				echo "Error: --comm-plugin requires a value (brpc|hcom)" >&2
				exit 1
			fi
			set_comm_plugin "$3"
			shift 2
			;;
		--comm-plugin=*)
			set_comm_plugin "${2#*=}"
			shift
			;;
		*)
			# Only break if this isn't the combined build case
			[[ -z "${2:-}" || "$2" == "pg" || "$2" == "falcon" ]] && break
			echo "Error: Combined build only supports --debug, --deploy, --coverage or --comm-plugin" >&2
			exit 1
			;;
		esac
	done

	case "${2:-}" in
	falcon)
		shift 2
		while [[ $# -gt 0 ]]; do
			case "$1" in
			--debug)
				BUILD_TYPE="Debug"
				;;
			--release | --deploy)
				BUILD_TYPE="Release"
				;;
			--relwithdebinfo)
				BUILD_TYPE="RelWithDebInfo"
				;;
			--coverage)
				COVERAGE=true
				;;
			--with-fuse-opt)
				WITH_FUSE_OPT=true
				;;
			--with-zk-init)
				WITH_ZK_INIT=true
				;;
			--with-rdma)
				WITH_RDMA=true
				;;
			--with-prometheus)
				WITH_PROMETHEUS=true
				;;
			--with-obs-storage)
				WITH_OBS_STORAGE=true
				;;
			--with-asan)
				WITH_ASAN=true
				;;
			--comm-plugin)
				if [[ -z "${2:-}" ]]; then
					echo "Error: --comm-plugin requires a value (brpc|hcom)" >&2
					exit 1
				fi
				set_comm_plugin "$2"
				shift
				;;
			--comm-plugin=*)
				set_comm_plugin "${1#*=}"
				;;
			--help | -h)
				echo "Usage: $0 build falcon [options]"
				echo ""
				echo "Build FalconFS Components"
				echo ""
				echo "Options:"
				echo "  --debug              Build in debug mode"
				echo "  --release            Build in release mode"
				echo "  --relwithdebinfo     Build with debug symbols"
				echo "  --coverage           Build with gcov/lcov instrumentation"
				echo "  --comm-plugin=PLUGIN Communication plugin: brpc (default) or hcom"
				echo "  --with-fuse-opt      Enable FUSE optimizations"
				echo "  --with-zk-init       Enable Zookeeper initialization for containerized deployment"
				echo "  --with-rdma          Enable RDMA support"
				echo "  --with-prometheus    Enable Prometheus metrics"
				echo "  --with-obs-storage   Enable OBS storage"
				echo "  --with-asan          Enable AddressSanitizer with dynamic linking for memory debugging"
				exit 0
				;;
			*)
				echo "Unknown option: $1"
				exit 1
				;;
			esac
			shift
		done
		build_falconfs
		;;
	*)
		build_falconfs
		;;
	esac
	;;
clean)
	case "${2:-}" in
	falcon)
		# Check for --help in clean falcon
		for arg in "${@:3}"; do
			if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
				echo "Usage: $0 clean falcon"
				echo "Clean FalconFS build artifacts"
				exit 0
			fi
		done
		clean_falconfs
		;;
	test)
		# Check for --help in clean test
		for arg in "${@:3}"; do
			if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
				echo "Usage: $0 clean test"
				echo "Clean test binaries"
				exit 0
			fi
		done
		clean_tests
		;;
	coverage)
		clean_coverage_data
		;;
	*)
		# Main clean command options
		while true; do
			case "${2:-}" in
			--help | -h)
				print_help "clean"
				exit 0
				;;
			*) break ;;
			esac
		done
		clean_falconfs
		;;
	esac
	;;
test)
	run_unit_tests
	;;
coverage)
	shift
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--help | -h)
			print_help "coverage"
			exit 0
			;;
		--local-run)
			RUN_LOCAL_SERVICE_FOR_COVERAGE=true
			;;
		*)
			echo "Unknown option for coverage: $1" >&2
			exit 1
			;;
		esac
		shift
	done
	run_coverage
	;;
install)
	case "${2:-}" in
	falcon)
		install_falcon_meta
		install_falcon_client
		install_falcon_python_sdk
		install_falcon_cm
		install_falcon_cn
		install_falcon_dn
		install_falcon_store
		install_falcon_regress
		install_private_directory_test
		install_deploy_scripts
		;;
	*)
		install_falcon_meta
		install_falcon_client
		install_falcon_python_sdk
		install_falcon_cm
		install_falcon_cn
		install_falcon_dn
		install_falcon_store
		install_falcon_regress
		install_private_directory_test
		install_deploy_scripts
		;;
	esac
	;;
*)
	print_help "build"
	exit 1
	;;
esac
