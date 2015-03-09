package kaltura.analytics.test.com;

import kaltura.analytics.test.env.SimParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class LiveEventsCassandraDriver
{
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
    //private DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

    public static final String COL_NAME_PARTNER_ID = "partner_id";
    public static final String COL_NAME_REFERRER = "referrer";
    public static final String COL_NAME_ENTRY_ID = "entry_id";
    public static final String COL_NAME_EVENT_TIME = "event_time";
    public static final String COL_NAME_COUNTRY = "country";
    public static final String COL_NAME_CITY = "city";
    public static final String COL_NAME_PLAYS = "plays";
    public static final String COL_NAME_ALIVE = "alive";
    public static final String COL_NAME_BITRATE = "bitrate";
    public static final String COL_NAME_BITRATE_COUNT = "bitrate_count";
    public static final String COL_NAME_BUFFER_TIME = "buffer_time";


    private static final Logger logger = LoggerFactory.getLogger(LiveEventsCassandraDriver.class);

    private AstyanaxContext<Keyspace> context;
    private Keyspace keyspace;

    private ColumnFamily<String, String> liveEventColumnFamily;
    private ColumnFamily<String, Long> liveEventLocationColumnFamily;
    private ColumnFamily<String, String> hourlyLiveEventColumnFamily;
    private ColumnFamily<String, String> hourlyLiveEventReferrerColumnFamily;
    private ColumnFamily<String, Long> hourlyLiveEventPartnerColumnFamily;

    private ColumnFamily<String, Long> livePartnerEntryColumnFamily;

    private static final String liveEventColumnFamilyName = "live_events";

    private static final String liveEventLocationColumnFamilyName = "live_events_location";

    private static final String hourlyLiveEventColumnFamilyName = "hourly_live_events";

    private static final String hourlyLiveEventReferrerColumnFamilyName = "hourly_live_events_referrer";

    private static final String hourlyLiveEventPartnerColumnFamilyName = "hourly_live_events_partner";

    private static final String livePartnerEntryColumnFamilyName = "live_partner_entry";

    private static final String liveEventInsertStatement =
            String.format("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?);",
                    liveEventColumnFamilyName, COL_NAME_ENTRY_ID, COL_NAME_EVENT_TIME, COL_NAME_PLAYS, COL_NAME_ALIVE, COL_NAME_BITRATE,
                    COL_NAME_BITRATE_COUNT, COL_NAME_BUFFER_TIME);

    private static final String liveEventLocationInsertStatement =
            String.format("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    liveEventLocationColumnFamilyName, COL_NAME_ENTRY_ID, COL_NAME_EVENT_TIME, COL_NAME_COUNTRY, COL_NAME_CITY,
                    COL_NAME_PLAYS, COL_NAME_ALIVE, COL_NAME_BITRATE, COL_NAME_BITRATE_COUNT, COL_NAME_BUFFER_TIME);

    private static final String hourlyLiveEventInsertStatement =
            String.format("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?);",
                    hourlyLiveEventColumnFamilyName, COL_NAME_ENTRY_ID, COL_NAME_EVENT_TIME, COL_NAME_PLAYS, COL_NAME_ALIVE, COL_NAME_BITRATE,
                    COL_NAME_BITRATE_COUNT, COL_NAME_BUFFER_TIME);

    private static final String hourlyLiveEventReferrerInsertStatement =
            String.format("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                    hourlyLiveEventReferrerColumnFamilyName, COL_NAME_ENTRY_ID, COL_NAME_EVENT_TIME, COL_NAME_REFERRER, COL_NAME_PLAYS, COL_NAME_ALIVE,
                    COL_NAME_BITRATE, COL_NAME_BITRATE_COUNT, COL_NAME_BUFFER_TIME);

    private static final String hourlyLiveEventPartnerInsertStatement =
            String.format("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?);",
                    hourlyLiveEventPartnerColumnFamilyName, COL_NAME_PARTNER_ID, COL_NAME_EVENT_TIME, COL_NAME_PLAYS, COL_NAME_ALIVE,
                    COL_NAME_BITRATE, COL_NAME_BITRATE_COUNT, COL_NAME_BUFFER_TIME);

    private static final String livePartnerEntryInsertStatement =
            String.format("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?);",
                    livePartnerEntryColumnFamilyName, COL_NAME_PARTNER_ID, COL_NAME_ENTRY_ID, COL_NAME_EVENT_TIME);

    private static final String liveEventCreateStatement =
            String.format("CREATE TABLE %s (%s text, %s timestamp, %s bigint, %s bigint, %s bigint, %s bigint, %s bigint, PRIMARY KEY (%s, %s))",
                    liveEventColumnFamilyName, COL_NAME_ENTRY_ID, COL_NAME_EVENT_TIME, COL_NAME_PLAYS, COL_NAME_ALIVE,
                    COL_NAME_BITRATE, COL_NAME_BITRATE_COUNT, COL_NAME_BUFFER_TIME, COL_NAME_ENTRY_ID, COL_NAME_EVENT_TIME);

    public void init( String clusterName, String keyspaceName, String ipAndPort )
    {
        logger.debug("init()");

        int MaxConnectionsPerHost = 50;

        context = new AstyanaxContext.Builder()
                .forCluster(clusterName)
                .forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                .setPort(9160)
                                .setMaxConnsPerHost(MaxConnectionsPerHost)
                                .setSeeds(ipAndPort)
                )
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setCqlVersion("3.0.0")
                        .setTargetCassandraVersion("2.0"))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        keyspace = context.getEntity();

        liveEventColumnFamily = ColumnFamily.newColumnFamily(
                liveEventColumnFamilyName,
                StringSerializer.get(),
                StringSerializer.get() );

        hourlyLiveEventColumnFamily = ColumnFamily.newColumnFamily(
                hourlyLiveEventColumnFamilyName,
                StringSerializer.get(),
                StringSerializer.get() );

        hourlyLiveEventReferrerColumnFamily = ColumnFamily.newColumnFamily(
                hourlyLiveEventReferrerColumnFamilyName,
                StringSerializer.get(),
                StringSerializer.get() );
    }

    public void insertLiveEvent( String entryId, long eventTime, long plays, long alive, long bitrate, long biterateCount, long bufferTime )
    {
        try
        {
            @SuppressWarnings("unused")
            OperationResult<CqlResult<String, String>> result = keyspace
                    .prepareQuery(liveEventColumnFamily)
                    .withCql(liveEventInsertStatement)
                    .asPreparedStatement()
                    .withStringValue(entryId)
                    .withLongValue(eventTime)
                    .withLongValue(plays)
                    .withLongValue(alive)
                    .withLongValue(bitrate)
                    .withLongValue(biterateCount)
                    .withLongValue(bufferTime)
                    .execute();
        }
        catch (ConnectionException e)
        {
            logger.error("failed to write data to C*", e);
            throw new RuntimeException("failed to write data to C*", e);
        }

        logger.debug("insert ok");
    }

    public void insertLiveEventLocation( String entryId, long eventTime, String country, String city, long plays,
                                         long alive, long bitrate, long biterateCount, long bufferTime )
    {
        try
        {
            @SuppressWarnings("unused")
            OperationResult<CqlResult<String, Long>> result = keyspace
                    .prepareQuery(liveEventLocationColumnFamily)
                    .withCql(liveEventLocationInsertStatement)
                    .asPreparedStatement()
                    .withStringValue(entryId)
                    .withLongValue(eventTime)
                    .withStringValue(country)
                    .withStringValue(city)
                    .withLongValue(plays)
                    .withLongValue(alive)
                    .withLongValue(bitrate)
                    .withLongValue(biterateCount)
                    .withLongValue(bufferTime)
                    .execute();
        }
        catch (ConnectionException e)
        {
            logger.error("failed to write data to C*", e);
            throw new RuntimeException("failed to write data to C*", e);
        }

        logger.debug("insert ok");
    }

    public void insertHourlyLiveEvent( String entryId, long eventTime, long plays, long alive, long bitrate,
                                       long biterateCount, long bufferTime )
    {
        try
        {
            @SuppressWarnings("unused")
            OperationResult<CqlResult<String, String>> result = keyspace
                    .prepareQuery(hourlyLiveEventColumnFamily)
                    .withCql(hourlyLiveEventInsertStatement)
                    .asPreparedStatement()
                    .withStringValue(entryId)
                    .withLongValue(eventTime)
                    .withLongValue(plays)
                    .withLongValue(alive)
                    .withLongValue(bitrate)
                    .withLongValue(biterateCount)
                    .withLongValue(bufferTime)
                    .execute();
        }
        catch (ConnectionException e)
        {
            logger.error("failed to write data to C*", e);
            throw new RuntimeException("failed to write data to C*", e);
        }

        logger.debug("insert ok");
    }

    public void insertHourlyLiveEventReferrer( String entryId, long eventTime, String referrer, long plays, long alive,
                                               long bitrate, long biterateCount, long bufferTime )
    {
        try
        {
            @SuppressWarnings("unused")
            OperationResult<CqlResult<String, String>> result = keyspace
                    .prepareQuery(hourlyLiveEventReferrerColumnFamily)
                    .withCql(hourlyLiveEventReferrerInsertStatement)
                    .asPreparedStatement()
                    .withStringValue(entryId)
                    .withLongValue(eventTime)
                    .withStringValue(referrer)
                    .withLongValue(plays)
                    .withLongValue(alive)
                    .withLongValue(bitrate)
                    .withLongValue(biterateCount)
                    .withLongValue(bufferTime)
                    .execute();
        }
        catch (ConnectionException e)
        {
            logger.error("failed to write data to C*", e);
            throw new RuntimeException("failed to write data to C*", e);
        }

        logger.debug("insert ok");
    }

    public void insertHourlyLiveEventPartner( int partnerId, long eventTime, long plays, long alive, long bitrate,
                                              long biterateCount, long bufferTime )
    {
        try
        {
            @SuppressWarnings("unused")
            OperationResult<CqlResult<String, Long>> result = keyspace
                    .prepareQuery(hourlyLiveEventPartnerColumnFamily)
                    .withCql(hourlyLiveEventPartnerInsertStatement)
                    .asPreparedStatement()
                    .withIntegerValue(partnerId)
                    .withLongValue(eventTime)
                    .withLongValue(plays)
                    .withLongValue(alive)
                    .withLongValue(bitrate)
                    .withLongValue(biterateCount)
                    .withLongValue(bufferTime)
                    .execute();
        }
        catch (ConnectionException e)
        {
            logger.error("failed to write data to C*", e);
            throw new RuntimeException("failed to write data to C*", e);
        }

        logger.debug("insert ok");
    }

    public void insertLivePartnerEntry( int partnerId, String entryId, long eventTime )
    {
        try
        {
            @SuppressWarnings("unused")
            OperationResult<CqlResult<String, Long>> result = keyspace
                    .prepareQuery(livePartnerEntryColumnFamily)
                    .withCql(livePartnerEntryInsertStatement)
                    .asPreparedStatement()
                    .withIntegerValue(partnerId)
                    .withStringValue(entryId)
                    .withLongValue(eventTime)
                    .execute();
        }
        catch (ConnectionException e)
        {
            logger.error("failed to write data to C*", e);
            throw new RuntimeException("failed to write data to C*", e);
        }

        logger.debug("insert ok");
    }

    public void insertDynamicProperties(int id, String[] ... entries)
    {
//        MutationBatch m = keyspace.prepareMutationBatch();
//
//        ColumnListMutation<String> clm = m.withRow(liveEventColumnFamily, id);
//        for(String[] kv : entries)
//        {
//            clm.putColumn(kv[0], kv[1], null);
//        }
//
//        try
//        {
//            @SuppressWarnings("unused")
//            OperationResult<Void> result = m.execute();
//        } catch (ConnectionException e)
//        {
//            logger.error("failed to write data to C*", e);
//            throw new RuntimeException("failed to write data to C*", e);
//        }
//
//        logger.debug("insert ok");
    }


    public void createCF()
    {
        logger.debug("CQL: "+ liveEventCreateStatement);

        try
        {
            @SuppressWarnings("unused")
            OperationResult<CqlResult<String, String>> result = keyspace
                    .prepareQuery(liveEventColumnFamily)
                    .withCql(liveEventCreateStatement)
                    .execute();
        }
        catch (ConnectionException e)
        {
            logger.error("failed to create CF", e);

            throw new RuntimeException("failed to create CF", e);
        }
    }

    public void readLiveEvent( String entryId )
    {
        logger.debug("read()");

        try
        {
            OperationResult<CqlResult<String, String>> result
                    = keyspace.prepareQuery(hourlyLiveEventColumnFamily)
                    .withCql(String.format("SELECT * FROM %s WHERE %s='%s';", liveEventColumnFamilyName, COL_NAME_ENTRY_ID, entryId))
                    .execute();

            for ( Row<String, String> row : result.getResult().getRows() )
            {
                logger.debug("row: "+row.getKey()+","+row); // why is rowKey null?
                System.out.println("row: "+row.getKey()+","+row);

                ColumnList<String> cols = row.getColumns();
                logger.debug("live event");

                System.out.println("- entry id: "+cols.getStringValue(COL_NAME_ENTRY_ID, null));
                System.out.println("- event time: "+cols.getLongValue(COL_NAME_EVENT_TIME, null));
                System.out.println("- plays: "+cols.getLongValue(COL_NAME_PLAYS, null));
                System.out.println("- alive: "+cols.getLongValue(COL_NAME_ALIVE, null));
                System.out.println("- bitrate: "+cols.getLongValue(COL_NAME_BITRATE, null));
                System.out.println("- bitrate count: "+cols.getLongValue(COL_NAME_BITRATE_COUNT, null));
                System.out.println("- buffer time: "+cols.getLongValue(COL_NAME_BUFFER_TIME, null));
//
//                logger.debug("- entry id: "+cols.getStringValue(COL_NAME_ENTRY_ID, null));
//                logger.debug("- event time: "+cols.getIntegerValue(COL_NAME_EVENT_TIME, null));
//                logger.debug("- plays: "+cols.getIntegerValue(COL_NAME_PLAYS, null));
//                logger.debug("- alive: "+cols.getIntegerValue(COL_NAME_ALIVE, null));
//                logger.debug("- bitrate: "+cols.getIntegerValue(COL_NAME_BITRATE, null));
//                logger.debug("- bitrate count: "+cols.getIntegerValue(COL_NAME_BITRATE_COUNT, null));
//                logger.debug("- buffer time: "+cols.getIntegerValue(COL_NAME_BUFFER_TIME, null));
            }
        }
        catch (ConnectionException e)
        {
            logger.error("failed to read from C*", e);

            throw new RuntimeException("failed to read from C*", e);
        }
    }

    public void validateEntriesLiveEvents( String[] entries, long time, long expectedAlive, long expectedPlays ) throws Exception
    {
        //logger.debug("readHourlyEntriesLiveEvents()");

        StringBuilder entriesBundle = new StringBuilder();

        for ( int i = 0; i < entries.length; i++)
        {
            entriesBundle.append("'");
            entriesBundle.append(entries[i]);
            entriesBundle.append("'");

            if ( i < entries.length - 1)
                entriesBundle.append(", ");
        }

        Date currentDateTime = new Date(time);

        String dateTimeStr = dateFormat.format(currentDateTime);

        try
        {

            //System.out.println(String.format("SELECT * FROM %s WHERE %s in (%s) and %s='%s';", liveEventColumnFamilyName, COL_NAME_ENTRY_ID, entriesBundle.toString(), COL_NAME_EVENT_TIME, dateTimeStr));

            OperationResult<CqlResult<String, String>> result
                    = keyspace.prepareQuery(liveEventColumnFamily)
                    .withCql(String.format("SELECT * FROM %s WHERE %s in (%s) and %s='%s';", liveEventColumnFamilyName, COL_NAME_ENTRY_ID, entriesBundle.toString(), COL_NAME_EVENT_TIME, dateTimeStr))
                    .execute();

            int nActualEntries = result.getResult().getRows().size();

            if ( ( nActualEntries > 0 ) && ( nActualEntries != entries.length ) )
            {
                logger.error(String.format("simulation failure time: %s actual num entries: %d expected num entries: %d", dateTimeStr, result.getResult().getRows().size(), entries.length));
                return;
            }


            for ( Row<String, String> row : result.getResult().getRows() )
            {
                //logger.debug("row: "+row.getKey()+","+row); // why is rowKey null?
                //System.out.println("row: "+row.getKey()+","+row);

                ColumnList<String> cols = row.getColumns();
                //logger.debug("live event");

                long actualAlive = cols.getLongValue(COL_NAME_ALIVE, null);
                long actualPlays = cols.getLongValue(COL_NAME_PLAYS, null);

                if ( actualAlive != expectedAlive )
                {
                    String entry = cols.getStringValue(COL_NAME_ENTRY_ID, null);

                    logger.error(String.format("simulation failure entry: %s time: %s actual alive: %d expected alive: %d", entry, dateTimeStr, actualAlive, expectedAlive));
                }

                if ( actualPlays != expectedPlays )
                {
                    String entry = cols.getStringValue(COL_NAME_ENTRY_ID, null);

                    logger.error(String.format("simulation failure entry: %s time: %s actual plays: %d expected plays: %d", entry, dateTimeStr, actualPlays, expectedPlays));
                }

                //System.out.println("validate OK");
//                System.out.println("- entry id: "+cols.getStringValue(COL_NAME_ENTRY_ID, null));
//                System.out.println("- event time: "+cols.getLongValue(COL_NAME_EVENT_TIME, null));
//                System.out.println("- plays: "+cols.getLongValue(COL_NAME_PLAYS, null));
//                System.out.println("- alive: "+cols.getLongValue(COL_NAME_ALIVE, null));
//                System.out.println("- bitrate: "+cols.getLongValue(COL_NAME_BITRATE, null));
//                System.out.println("- bitrate count: "+cols.getLongValue(COL_NAME_BITRATE_COUNT, null));
//                System.out.println("- buffer time: "+cols.getLongValue(COL_NAME_BUFFER_TIME, null));
            }

            System.out.println(String.format("validate time: %s OK", dateTimeStr));
        }
        catch (ConnectionException e)
        {
            logger.error("failed to read from C*", e);

            throw new RuntimeException("failed to read from C*", e);
        }
    }

    public void validateHourlyPartnerLiveEvents( String[] partners, long time, long expectedAlive, long expectedPlays ) throws Exception
    {
        //logger.debug("readHourlyEntriesLiveEvents()");

        StringBuilder partnersBundle = new StringBuilder();

        for ( int i = 0; i < partners.length; i++)
        {
            partnersBundle.append("'");
            partnersBundle.append(partners[i]);
            partnersBundle.append("'");

            if ( i < partners.length - 1)
                partnersBundle.append(", ");
        }

        Date currentDateTime = new Date(time);

        String dateTimeStr = dateFormat.format(currentDateTime);

        try
        {
            //System.out.println(String.format("SELECT * FROM %s WHERE %s in (%s) and %s='%s';", hourlyLiveEventColumnFamilyName, COL_NAME_ENTRY_ID, entriesBundle.toString(), COL_NAME_EVENT_TIME, dateTimeStr));

            OperationResult<CqlResult<String, String>> result
                    = keyspace.prepareQuery(hourlyLiveEventColumnFamily)
                    .withCql(String.format("SELECT * FROM %s WHERE %s in (%s) and %s='%s';", hourlyLiveEventColumnFamilyName, COL_NAME_ENTRY_ID, partnersBundle.toString(), COL_NAME_EVENT_TIME, dateTimeStr))
                    .execute();

            if ( result.getResult().getRows().size() != partners.length )
            {
                logger.error(String.format("hourly simulation failure time: %s actual num partners: %d expected num partners: %d", dateTimeStr, result.getResult().getRows().size(), partners.length));
                return;
            }

            for ( Row<String, String> row : result.getResult().getRows() )
            {
                //logger.debug("row: "+row.getKey()+","+row); // why is rowKey null?
                //System.out.println("row: "+row.getKey()+","+row);

                ColumnList<String> cols = row.getColumns();
                //logger.debug("live event");

                long actualAlive = cols.getLongValue(COL_NAME_ALIVE, null);
                long actualPlays = cols.getLongValue(COL_NAME_PLAYS, null);

                //System.out.println(String.format("hourly entries simulation entry: %s time: %s alive: %d plays: %d", cols.getStringValue(COL_NAME_ENTRY_ID, null), dateTimeStr, actualAlive, actualPlays));

                if ( actualAlive != expectedAlive )
                {
                    String entry = cols.getStringValue(COL_NAME_ENTRY_ID, null);

                    logger.error(String.format("hourly entries simulation failure partner: %s time: %s actual alive: %d expected alive: %d", entry, dateTimeStr, actualAlive, expectedAlive));
                }

                if ( actualPlays != expectedPlays )
                {
                    String entry = cols.getStringValue(COL_NAME_ENTRY_ID, null);

                    logger.error(String.format("hourly entries simulation failure partner: %s time: %s actual plays: %d expected plays: %d", entry, dateTimeStr, actualPlays, expectedPlays));
                }

//                System.out.println("- entry id: "+cols.getStringValue(COL_NAME_ENTRY_ID, null));
//                System.out.println("- event time: "+cols.getLongValue(COL_NAME_EVENT_TIME, null));
//                System.out.println("- plays: "+cols.getLongValue(COL_NAME_PLAYS, null));
//                System.out.println("- alive: "+cols.getLongValue(COL_NAME_ALIVE, null));
//                System.out.println("- bitrate: "+cols.getLongValue(COL_NAME_BITRATE, null));
//                System.out.println("- bitrate count: "+cols.getLongValue(COL_NAME_BITRATE_COUNT, null));
//                System.out.println("- buffer time: "+cols.getLongValue(COL_NAME_BUFFER_TIME, null));
            }

            System.out.println(String.format("hourly partners validate time: %s OK", dateTimeStr));
        }
        catch (ConnectionException e)
        {
            logger.error("failed to read from C*", e);

            throw new RuntimeException("failed to read from C*", e);
        }
    }

    public void validateHourlyEntriesLiveEvents( String[] entries, long time, long expectedAlive, long expectedPlays ) throws Exception
    {
        //logger.debug("readHourlyEntriesLiveEvents()");

        StringBuilder entriesBundle = new StringBuilder();

        for ( int i = 0; i < entries.length; i++)
        {
            entriesBundle.append("'");
            entriesBundle.append(entries[i]);
            entriesBundle.append("'");

            if ( i < entries.length - 1)
                entriesBundle.append(", ");
        }

        Date currentDateTime = new Date(time);

        String dateTimeStr = dateFormat.format(currentDateTime);

        try
        {
            //System.out.println(String.format("SELECT * FROM %s WHERE %s in (%s) and %s='%s';", hourlyLiveEventColumnFamilyName, COL_NAME_ENTRY_ID, entriesBundle.toString(), COL_NAME_EVENT_TIME, dateTimeStr));

            OperationResult<CqlResult<String, String>> result
                    = keyspace.prepareQuery(hourlyLiveEventColumnFamily)
                    .withCql(String.format("SELECT * FROM %s WHERE %s in (%s) and %s='%s';", hourlyLiveEventColumnFamilyName, COL_NAME_ENTRY_ID, entriesBundle.toString(), COL_NAME_EVENT_TIME, dateTimeStr))
                    .execute();

            if ( result.getResult().getRows().size() != entries.length )
            {
                logger.error(String.format("hourly simulation failure time: %s actual num entries: %d expected num entries: %d", dateTimeStr, result.getResult().getRows().size(), entries.length));
                return;
            }

            for ( Row<String, String> row : result.getResult().getRows() )
            {
                //logger.debug("row: "+row.getKey()+","+row); // why is rowKey null?
                //System.out.println("row: "+row.getKey()+","+row);

                ColumnList<String> cols = row.getColumns();
                //logger.debug("live event");

                long actualAlive = cols.getLongValue(COL_NAME_ALIVE, null);
                long actualPlays = cols.getLongValue(COL_NAME_PLAYS, null);

                //System.out.println(String.format("hourly entries simulation entry: %s time: %s alive: %d plays: %d", cols.getStringValue(COL_NAME_ENTRY_ID, null), dateTimeStr, actualAlive, actualPlays));

                if ( actualAlive != expectedAlive )
                {
                    String entry = cols.getStringValue(COL_NAME_ENTRY_ID, null);

                    logger.error(String.format("hourly entries simulation failure entry: %s time: %s actual alive: %d expected alive: %d", entry, dateTimeStr, actualAlive, expectedAlive));
                }

                if ( actualPlays != expectedPlays )
                {
                    String entry = cols.getStringValue(COL_NAME_ENTRY_ID, null);

                    logger.error(String.format("hourly entries simulation failure entry: %s time: %s actual plays: %d expected plays: %d", entry, dateTimeStr, actualPlays, expectedPlays));
                }

//                System.out.println("- entry id: "+cols.getStringValue(COL_NAME_ENTRY_ID, null));
//                System.out.println("- event time: "+cols.getLongValue(COL_NAME_EVENT_TIME, null));
//                System.out.println("- plays: "+cols.getLongValue(COL_NAME_PLAYS, null));
//                System.out.println("- alive: "+cols.getLongValue(COL_NAME_ALIVE, null));
//                System.out.println("- bitrate: "+cols.getLongValue(COL_NAME_BITRATE, null));
//                System.out.println("- bitrate count: "+cols.getLongValue(COL_NAME_BITRATE_COUNT, null));
//                System.out.println("- buffer time: "+cols.getLongValue(COL_NAME_BUFFER_TIME, null));
            }

            System.out.println(String.format("hourly entries validate time: %s OK", dateTimeStr));
        }
        catch (ConnectionException e)
        {
            logger.error("failed to read from C*", e);

            throw new RuntimeException("failed to read from C*", e);
        }
    }

    public void validateHourlyReferrersLiveEvents( String[] entries, long time, long expectedAlive, long expectedPlays ) throws Exception
    {
        //logger.debug("validateHourlyReferrersLiveEvents()");

        Date currentDateTime = new Date(time);

        String dateTimeStr = dateFormat.format(currentDateTime);

        try
        {
            for ( int i = 0; i < entries.length; i++)
            {
                //System.out.println(String.format("SELECT * FROM %s WHERE %s='%s' and %s='%s';", hourlyLiveEventReferrerColumnFamilyName, COL_NAME_ENTRY_ID, entries[i], COL_NAME_EVENT_TIME, dateTimeStr));

                OperationResult<CqlResult<String, String>> result
                        = keyspace.prepareQuery(hourlyLiveEventReferrerColumnFamily)
                        .withCql(String.format("SELECT * FROM %s WHERE %s='%s' and %s='%s';", hourlyLiveEventReferrerColumnFamilyName, COL_NAME_ENTRY_ID, entries[i], COL_NAME_EVENT_TIME, dateTimeStr))
                        .execute();

                if ( result.getResult().getRows().size() != SimParams._nReferrers() )
                {
                    logger.error(String.format("hourly referrers simulation failure time: %s actual num referrers: %d expected num referrers: %d", dateTimeStr, result.getResult().getRows().size(), SimParams._nReferrers()));
                    return;
                }

                for ( Row<String, String> row : result.getResult().getRows() )
                {
                    //logger.debug("row: "+row.getKey()+","+row); // why is rowKey null?
                    //System.out.println("row: "+row.getKey()+","+row);

                    ColumnList<String> cols = row.getColumns();
                    //logger.debug("live event");

                    long actualAlive = cols.getLongValue(COL_NAME_ALIVE, null);
                    long actualPlays = cols.getLongValue(COL_NAME_PLAYS, null);

                    //System.out.println(String.format("hourly referrers simulation entry: %s referrer: %s time: %s alive: %d plays: %d", entries[i], cols.getStringValue(COL_NAME_REFERRER, null), dateTimeStr, actualAlive, actualPlays));

                    if ( actualAlive != expectedAlive )
                    {
                        String referrer = cols.getStringValue(COL_NAME_REFERRER, null);

                        logger.error(String.format("hourly referrers simulation failure entry: %s referrer: %s time: %s actual alive: %d expected alive: %d", entries[i], referrer, dateTimeStr, actualAlive, expectedAlive));
                    }

                    if ( actualPlays != expectedPlays )
                    {
                        String referrer = cols.getStringValue(COL_NAME_REFERRER, null);

                        logger.error(String.format("hourly referrers simulation failure entry: %s referrer: %s time: %s actual plays: %d expected plays: %d", entries[i], referrer, dateTimeStr, actualPlays, expectedPlays));
                    }

//                System.out.println("- entry id: "+cols.getStringValue(COL_NAME_ENTRY_ID, null));
//                System.out.println("- event time: "+cols.getLongValue(COL_NAME_EVENT_TIME, null));
//                System.out.println("- plays: "+cols.getLongValue(COL_NAME_PLAYS, null));
//                System.out.println("- alive: "+cols.getLongValue(COL_NAME_ALIVE, null));
//                System.out.println("- bitrate: "+cols.getLongValue(COL_NAME_BITRATE, null));
//                System.out.println("- bitrate count: "+cols.getLongValue(COL_NAME_BITRATE_COUNT, null));
//                System.out.println("- buffer time: "+cols.getLongValue(COL_NAME_BUFFER_TIME, null));
                }
            }

            System.out.println(String.format("hourly referrers validate time: %s OK", dateTimeStr));
        }
        catch (ConnectionException e)
        {
            logger.error("failed to read from C*", e);

            throw new RuntimeException("failed to read from C*", e);
        }
    }

//    public void debugReadHourly(long time) throws Exception
//    {
//        //logger.debug("readHourlyEntriesLiveEvents()");
//
//        Date currentDateTime = new Date(time);
//
//        String dateTimeStr = dateFormat.format(currentDateTime);
//
//        try
//        {
//            System.out.println(String.format("SELECT * FROM %s limit 10;", hourlyLiveEventColumnFamilyName));
//
////            OperationResult<CqlResult<String, String>> result
////                    = keyspace.prepareQuery(hourlyLiveEventColumnFamily)
////                    .withCql(String.format("SELECT * FROM %s WHERE %s in (%s) and %s=%d;", hourlyLiveEventColumnFamilyName, COL_NAME_ENTRY_ID, entriesBundle.toString(), COL_NAME_EVENT_TIME, time))
////                    .execute();
//
//            OperationResult<CqlResult<String, String>> result
//                    = keyspace.prepareQuery(hourlyLiveEventColumnFamily)
//                    .withCql(String.format("SELECT * FROM %s limit 10;", hourlyLiveEventColumnFamilyName))
//                    .execute();
//
//            System.out.println(String.format("hourly entries validate time: %s OK", dateTimeStr));
//        }
//        catch (ConnectionException e)
//        {
//            logger.error("failed to read from C*", e);
//
//            throw new RuntimeException("failed to read from C*", e);
//        }
//    }

//    public void test()
//    {
//        long time = System.currentTimeMillis();
//        //insertLiveEvent("test", time, 10, 11, 12, 13, 14);
//        insertLiveEvent("test", time, 333, 0, 0, 0, 0);
//
//        readLiveEvent("test");
//    }
//
//    public void run()
//    {
//        logger.debug("main");
//        LiveEventsCassandraDriver c = new LiveEventsCassandraDriver();
//        c.init("Test Cluster", "kaltura_live", "127.0.0.1:9160");
//        //c.createCF();
//
//        //for ( int i = 0; i < 20000; i++ )
//
//        long time = System.currentTimeMillis();
//        c.insertLiveEvent("test", time, 10, 11, 12, 13, 14);
//
//        c.readLiveEvent("test");
//    }

}
