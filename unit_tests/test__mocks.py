import sys
from unittest.mock import Mock

# This file is called 'test__mocks' because:
# 'test' so it is initialised by pytest
# '__' to guarantee that it is initialised before other test files

sys.modules["delta.tables"] = Mock()
sys.modules["pyspark.sql.functions"] = functions_mock = Mock(
    hash=Mock(return_value="mock hash")
)
