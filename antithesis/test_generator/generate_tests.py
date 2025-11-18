import os
import shutil
import json
import argparse
from collections import defaultdict
from ducktape.tests.loader import TestLoader
from ducktape.tests.session import SessionContext, SessionLoggerMaker

def main():
    parser = argparse.ArgumentParser(description='Generate test scripts for Antithesis')
    parser.add_argument('input_symbols', nargs='+',
                       help='One or more test symbols to load (e.g., test_module.TestClass.test_method)')
    parser.add_argument('output_dir', 
                       help='Output directory for generated test scripts')
    
    args = parser.parse_args()

    session_id = "arbitrary"
    session_context = SessionContext(session_id=session_id, results_dir=".")
    session_logger = SessionLoggerMaker(session_context).logger
    loader = TestLoader(
        session_context=session_context,
        logger=session_logger,
    )

    grouped_tests = defaultdict(list)
    for t in loader.load(symbols=args.input_symbols):
        key = (t.file, t.cls.__name__, t.function.__name__)
        grouped_tests[key].append(t)

    output_dir = args.output_dir
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    antithesis_test_dir = os.path.abspath(output_dir)
    os.makedirs(antithesis_test_dir, exist_ok=True)

    for (file, cls, function), tests in grouped_tests.items():
        # Extract relative path from the absolute file path
        # The file path from the test object is absolute, we need to convert it to relative
        # Find the "tests" directory in the path and use everything from there
        file_parts = file.split("/")
        try:
            tests_index = file_parts.index("tests")
            relative_file = "/".join(file_parts[tests_index:])
        except ValueError:
            # If "tests" not found, this indicates a serious problem with the test file structure
            raise ValueError(f"Test file path does not contain 'tests' directory: {file}")
        
        path_parts = relative_file.split("/")
        if len(path_parts) >= 4 and path_parts[0] == "tests" and path_parts[1] == "kafkatest" and path_parts[2] == "tests":
            test_category = path_parts[3]
        else:
            raise ValueError(f"Test file path does not match expected structure 'tests/kafkatest/tests/{{category}}/': {relative_file}")

        test_category_dir = os.path.join(antithesis_test_dir, test_category)
        os.makedirs(test_category_dir, exist_ok=True)

        for i, test in enumerate(tests, 1):
            test_file_name = os.path.splitext(os.path.basename(file))[0]
            filename = f"singleton_driver__{test_file_name}__{cls}__{function}__{i}.sh"
            filepath = os.path.join(test_category_dir, filename)

            # Only include @params if there are actual parameters
            params_suffix = ""
            if hasattr(test, 'injected_args') and test.injected_args:
                params_suffix = f"@{json.dumps(test.injected_args)}"
            
            script_content = f"""#!/bin/bash
set -ex

cd /opt/kafka-dev/
ducktape --cluster-file /opt/kafka-dev/cluster.json \\
  '/opt/kafka-dev/{relative_file}::{cls}.{function}{params_suffix}'"""

            with open(filepath, 'w') as f:
                f.write(script_content + '\n')
            os.chmod(filepath, 0o755)

if __name__ == "__main__":
    main()
