package org.opencb.opencga.storage.hadoop.variant.annotation.phoenix;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.UpsertExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public abstract class OpenCGAUpsertExecutor<RECORD, FIELD> extends UpsertExecutor<RECORD, FIELD> {

    public OpenCGAUpsertExecutor(Connection conn, String tableName, List<ColumnInfo> columnInfoList,
                                 UpsertListener<RECORD> upsertListener) {
        super(conn, tableName, columnInfoList, upsertListener);
    }

    protected OpenCGAUpsertExecutor(Connection conn, List<ColumnInfo> columnInfoList, PreparedStatement preparedStatement,
                                    UpsertListener<RECORD> upsertListener) {
        super(conn, columnInfoList, preparedStatement, upsertListener);
    }

    @Override
    protected org.apache.phoenix.thirdparty.com.google.common.base.Function<FIELD, Object> createConversionFunction(PDataType dataType) {
        java.util.function.Function<FIELD, Object> f = createJavaConversionFunction(dataType);
        return f::apply;
    }

    protected abstract java.util.function.Function<FIELD, Object> createJavaConversionFunction(PDataType dataType);
}
