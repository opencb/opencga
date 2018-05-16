package org.opencb.opencga.storage.hadoop.variant.index;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantSqlQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created on 11/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantPhoenixCursorIterator extends VariantDBIterator {

    private PreparedStatement statement;
    private ResultSet resultSet;
    private final HBaseToVariantConverter<ResultSet> converter;
    private final Logger logger = LoggerFactory.getLogger(VariantHBaseResultSetIterator.class);

    private Boolean hasNext = null;
    private String cursorName;

    public VariantPhoenixCursorIterator(
            VariantSqlQueryParser.VariantPhoenixSQLQuery query, Connection connection, GenomeHelper genomeHelper,
            StudyConfigurationManager scm, List<String> formats, String unknownGenotype, QueryOptions options) {
        this(query, connection, HBaseToVariantConverter.fromResultSet(genomeHelper, scm)
                .setSelectVariantElements(query.getSelect())
                .setMutableSamplesPosition(false)
                .setStudyNameAsStudyId(options.getBoolean(HBaseToVariantConverter.STUDY_NAME_AS_STUDY_ID, true))
                .setUnknownGenotype(unknownGenotype)
                .setSimpleGenotypes(options.getBoolean(HBaseToVariantConverter.SIMPLE_GENOTYPES, true))
                .setFormats(formats));
    }

    public VariantPhoenixCursorIterator(VariantSqlQueryParser.VariantPhoenixSQLQuery query, Connection connection,
                                        HBaseToVariantConverter<ResultSet> converter) {
        try {
            cursorName = "tCursor"; // TODO: Randomize name
            statement = connection.prepareStatement("DECLARE " + cursorName + " CURSOR FOR " + query.getSql());
            statement.execute();

            statement = connection.prepareStatement("OPEN " + cursorName);
            statement.execute();


            statement = connection.prepareStatement("FETCH NEXT 10 FROM " + cursorName);
            resultSet = null;


            this.converter = converter;

        } catch (SQLException e) {
            throw VariantQueryException.internalException(e);
        }
    }


    @Override
    public void close() throws Exception {
        super.close();
        logger.debug("Close variant iterator. Fetch = {}ms, Convert = {}ms",
                getTimeFetching() / 1000000.0, getTimeConverting() / 1000000.0);
        statement.execute("CLOSE " + cursorName);
        statement.execute();

        resultSet.close();
        statement.close();
    }


    private ResultSet getNextResultSet() {
        try {
            logger.info("Execute query!");
            this.resultSet = statement.executeQuery();
        } catch (SQLException e) {
            throw VariantQueryException.internalException(e);
        }
        return resultSet;
    }

    protected ResultSet getResultSet() {
        if (resultSet == null) {
            getNextResultSet();
        }
        return resultSet;
    }

    @Override
    public boolean hasNext() {
        if (hasNext == null) {
            try {
                hasNext = fetch(() -> getResultSet().next());
                if (!hasNext) {
                    resultSet.close();
                    hasNext = fetch(() -> getNextResultSet().next());
                }
            } catch (SQLException e) {
                throw VariantQueryException.internalException(e);
            }
        }
        return hasNext;
    }

    @Override
    public Variant next() {
        Variant variant = convert(() -> converter.convert(resultSet));
        hasNext = null;
        return variant;
    }
}
