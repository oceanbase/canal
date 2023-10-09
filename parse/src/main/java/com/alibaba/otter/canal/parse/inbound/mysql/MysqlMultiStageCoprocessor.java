package com.alibaba.otter.canal.parse.inbound.mysql;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.compress.utils.Lists;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.AbstractMultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.EventTransactionBuffer;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkerPool;
import com.taobao.tddl.dbsync.binlog.LogBuffer;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.*;

/**
 * 针对解析器提供一个多阶段协同的处理
 *
 * <pre>
 * 1. 网络接收 (单线程)
 * 2. 事件基本解析 (单线程，事件类型、DDL解析构造TableMeta、维护位点信息)
 * 3. 事件深度解析 (多线程, DML事件数据的完整解析)
 * 4. 投递到store (单线程)
 * </pre>
 *
 * @author agapple 2018年7月3日 下午4:54:17
 * @since 1.0.26
 */
public class MysqlMultiStageCoprocessor extends AbstractMultiStageCoprocessor<MysqlMultiStageCoprocessor.MessageEvent> {

    private final int                         parserThreadCount;
    private final LogEventConvert             logEventConvert;
    private boolean                           filterDmlInsert = false;
    private boolean                           filterDmlUpdate = false;
    private boolean                           filterDmlDelete = false;
    private ErosaConnection                   connection;
    private GTIDSet                           gtidSet;
    private LogContext                        logContext;

    public MysqlMultiStageCoprocessor(int ringBufferSize, int parserThreadCount, LogEventConvert logEventConvert,
                                      EventTransactionBuffer transactionBuffer, String destination,
                                      boolean filterDmlInsert, boolean filterDmlUpdate, boolean filterDmlDelete){
        super(destination, transactionBuffer, ringBufferSize);
        this.parserThreadCount = parserThreadCount;
        this.logEventConvert = logEventConvert;
        this.filterDmlInsert = filterDmlInsert;
        this.filterDmlUpdate = filterDmlUpdate;
        this.filterDmlDelete = filterDmlDelete;
    }

    @Override
    public void start() {
        super.start();
        disruptorMsgBuffer = RingBuffer.createSingleProducer(new MessageEventFactory(),
            ringBufferSize,
            new BlockingWaitStrategy());
        int tc = parserThreadCount > 0 ? parserThreadCount : 1;
        ExecutorService parserExecutor = Executors.newFixedThreadPool(tc, new NamedThreadFactory("MultiStageCoprocessor-Parser-"
                                                                                                 + destination));
        executorServices.add(parserExecutor);
        ExecutorService stageExecutor = Executors.newFixedThreadPool(2, new NamedThreadFactory("MultiStageCoprocessor-other-"
                                                                                    + destination));
        executorServices.add(stageExecutor);

        SequenceBarrier sequenceBarrier = disruptorMsgBuffer.newBarrier();
        ExceptionHandler<MessageEvent> exceptionHandler = new SimpleFatalExceptionHandler();
        // stage 2
        this.logContext = new LogContext();
        BatchEventProcessor<MessageEvent> simpleParserStage = new BatchEventProcessor<>(disruptorMsgBuffer,
            sequenceBarrier,
            new SimpleParserStage(logContext));
        simpleParserStage.setExceptionHandler(exceptionHandler);
        eventProcessors.add(simpleParserStage);
        disruptorMsgBuffer.addGatingSequences(simpleParserStage.getSequence());

        // stage 3
        SequenceBarrier dmlParserSequenceBarrier = disruptorMsgBuffer.newBarrier(simpleParserStage.getSequence());
        WorkHandler<MessageEvent>[] workHandlers = new DmlParserStage[tc];
        for (int i = 0; i < tc; i++) {
            workHandlers[i] = new DmlParserStage();
        }
        WorkerPool<MessageEvent> workerPool = new WorkerPool<>(disruptorMsgBuffer,
            dmlParserSequenceBarrier,
            exceptionHandler,
            workHandlers);
        workerPools.add(workerPool);
        Sequence[] sequence = workerPool.getWorkerSequences();
        disruptorMsgBuffer.addGatingSequences(sequence);

        // stage 4
        SequenceBarrier sinkSequenceBarrier = disruptorMsgBuffer.newBarrier(sequence);
        BatchEventProcessor<MessageEvent> sinkStoreStage = new BatchEventProcessor<>(disruptorMsgBuffer, sinkSequenceBarrier, new SinkStoreStage());
        sinkStoreStage.setExceptionHandler(exceptionHandler);
        eventProcessors.add(sinkStoreStage);
        disruptorMsgBuffer.addGatingSequences(sinkStoreStage.getSequence());

        // start work
        stageExecutor.submit(simpleParserStage);
        stageExecutor.submit(sinkStoreStage);
        workerPool.start(parserExecutor);
    }

    @Override
    protected void setEventData(MessageEvent messageEvent, Object data) {
        if (data instanceof LogBuffer) {
            messageEvent.setBuffer((LogBuffer) data);
        } else if (data instanceof  LogEvent) {
            messageEvent.setEvent((LogEvent) data);
        }
    }

    public void setBinlogChecksum(int binlogChecksum) {
        if (binlogChecksum != LogEvent.BINLOG_CHECKSUM_ALG_OFF) {
            logContext.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum));
        }
    }

    private class SimpleParserStage implements EventHandler<MessageEvent>, LifecycleAware {

        private LogDecoder decoder;
        private LogContext context;

        public SimpleParserStage(LogContext context){
            decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            this.context = context;
            if (gtidSet != null) {
                context.setGtidSet(gtidSet);
            }
        }

        @Override
        public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                LogEvent logEvent = event.getEvent();
                if (logEvent == null) {
                    LogBuffer buffer = event.getBuffer();
                    logEvent = decoder.decode(buffer, context);
                    event.setEvent(logEvent);
                }

                int eventType = logEvent.getHeader().getType();
                boolean needIterate = false;

                if (eventType == LogEvent.TRANSACTION_PAYLOAD_EVENT) {
                    // https://github.com/alibaba/canal/issues/4388
                    List<LogEvent> deLogEvents = decoder.processIterateDecode(logEvent, context);
                    List<TableMeta> tableMetas = Lists.newArrayList();
                    event.setNeedIterate(true);
                    for (LogEvent deLogEvent : deLogEvents) {
                        TableMeta table = processEvent(deLogEvent, event);
                        tableMetas.add(table);
                    }
                    event.setIterateEvents(deLogEvents);
                    event.setIterateTables(tableMetas);
                } else {
                    TableMeta table = processEvent(logEvent, event);
                    event.setTable(table);
                }
            } catch (Throwable e) {
                exception = new CanalParseException(e);
                throw exception;
            }
        }

        private TableMeta processEvent(LogEvent logEvent, MessageEvent event) {
            TableMeta tableMeta = null;
            boolean needDmlParse = false;
            int eventType = logEvent.getHeader().getType();
            switch (eventType) {
                case LogEvent.WRITE_ROWS_EVENT_V1:
                case LogEvent.WRITE_ROWS_EVENT:
                    if (!filterDmlInsert) {
                        tableMeta = logEventConvert.parseRowsEventForTableMeta((WriteRowsLogEvent) logEvent);
                        needDmlParse = true;
                    }
                    break;
                case LogEvent.UPDATE_ROWS_EVENT_V1:
                case LogEvent.PARTIAL_UPDATE_ROWS_EVENT:
                case LogEvent.UPDATE_ROWS_EVENT:
                    if (!filterDmlUpdate) {
                        tableMeta = logEventConvert.parseRowsEventForTableMeta((UpdateRowsLogEvent) logEvent);
                        needDmlParse = true;
                    }
                    break;
                case LogEvent.DELETE_ROWS_EVENT_V1:
                case LogEvent.DELETE_ROWS_EVENT:
                    if (!filterDmlDelete) {
                        tableMeta = logEventConvert.parseRowsEventForTableMeta((DeleteRowsLogEvent) logEvent);
                        needDmlParse = true;
                    }
                    break;
                case LogEvent.ROWS_QUERY_LOG_EVENT:
                    needDmlParse = true;
                    break;
                default:
                    CanalEntry.Entry entry = logEventConvert.parse(event.getEvent(), false);
                    event.setEntry(entry);
            }

            // 记录一下DML的表结构
            if (needDmlParse && !event.isNeedDmlParse()) {
                event.setNeedDmlParse(true);
            }
            return tableMeta;
        }

        @Override
        public void onStart() {

        }

        @Override
        public void onShutdown() {

        }
    }

    private class DmlParserStage implements WorkHandler<MessageEvent>, LifecycleAware {

        @Override
        public void onEvent(MessageEvent event) throws Exception {
            try {
                if (event.isNeedDmlParse()) {
                    if (event.isNeedIterate()) {
                        // compress binlog
                        List<CanalEntry.Entry> entrys = Lists.newArrayList();
                        for (int index = 0; index < event.getIterateEvents().size(); index++) {
                            CanalEntry.Entry entry = processEvent(event.getIterateEvents().get(index),
                                event.getIterateTables().get(index));
                            if (entry != null) {
                                entrys.add(entry);
                            }
                        }
                        event.setIterateEntrys(entrys);
                    } else {
                        CanalEntry.Entry entry = processEvent(event.getEvent(), event.getTable());
                        event.setEntry(entry);
                    }
                }
            } catch (Throwable e) {
                exception = new CanalParseException(e);
                throw exception;
            }
        }

        private CanalEntry.Entry processEvent(LogEvent logEvent, TableMeta table) {
            int eventType = logEvent.getHeader().getType();
            CanalEntry.Entry entry = null;
            switch (eventType) {
                case LogEvent.WRITE_ROWS_EVENT_V1:
                case LogEvent.WRITE_ROWS_EVENT:
                case LogEvent.UPDATE_ROWS_EVENT_V1:
                case LogEvent.PARTIAL_UPDATE_ROWS_EVENT:
                case LogEvent.UPDATE_ROWS_EVENT:
                case LogEvent.DELETE_ROWS_EVENT_V1:
                case LogEvent.DELETE_ROWS_EVENT:
                    // 单独解析dml事件
                    entry = logEventConvert.parseRowsEvent((RowsLogEvent) logEvent, table);
                    break;
                default:
                    // 如果出现compress binlog,会出现其他的event type类型
                    entry = logEventConvert.parse(logEvent, false);
                    break;
            }

            return entry;
        }

        @Override
        public void onStart() {

        }

        @Override
        public void onShutdown() {

        }
    }

    private class SinkStoreStage implements EventHandler<MessageEvent>, LifecycleAware {

        @Override
        public void onEvent(MessageEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                if (event.isNeedIterate()) {
                    // compress binlog
                    for (CanalEntry.Entry entry : event.getIterateEntrys()) {
                        transactionBuffer.add(entry);
                    }
                } else {
                    if (event.getEntry() != null) {
                        transactionBuffer.add(event.getEntry());
                    }
                }

                LogEvent logEvent = event.getEvent();
                if (connection instanceof MysqlConnection && logEvent.getSemival() == 1) {
                    // semi ack回报
                    ((MysqlConnection) connection).sendSemiAck(logEvent.getHeader().getLogFileName(),
                        logEvent.getHeader().getLogPos());
                }

                // clear for gc
                event.setBuffer(null);
                event.setEvent(null);
                event.setTable(null);
                event.setEntry(null);
                // clear compress binlog events
                event.setNeedDmlParse(false);
                event.setNeedIterate(false);
                event.setIterateEntrys(null);
                event.setIterateTables(null);
                event.setIterateEvents(null);
            } catch (Throwable e) {
                exception = new CanalParseException(e);
                throw exception;
            }
        }

        @Override
        public void onStart() {

        }

        @Override
        public void onShutdown() {

        }
    }

    static class MessageEvent {

        private LogBuffer        buffer;
        private CanalEntry.Entry entry;
        private boolean          needDmlParse = false;
        private TableMeta        table;
        private LogEvent         event;
        private boolean                needIterate  = false;
        // compress binlog
        private List<LogEvent>         iterateEvents;
        private List<TableMeta>        iterateTables;
        private List<CanalEntry.Entry> iterateEntrys;

        public LogBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(LogBuffer buffer) {
            this.buffer = buffer;
        }

        public LogEvent getEvent() {
            return event;
        }

        public void setEvent(LogEvent event) {
            this.event = event;
        }

        public CanalEntry.Entry getEntry() {
            return entry;
        }

        public void setEntry(CanalEntry.Entry entry) {
            this.entry = entry;
        }

        public boolean isNeedDmlParse() {
            return needDmlParse;
        }

        public void setNeedDmlParse(boolean needDmlParse) {
            this.needDmlParse = needDmlParse;
        }

        public TableMeta getTable() {
            return table;
        }

        public void setTable(TableMeta table) {
            this.table = table;
        }

        public boolean isNeedIterate() {
            return needIterate;
        }

        public void setNeedIterate(boolean needIterate) {
            this.needIterate = needIterate;
        }

        public List<LogEvent> getIterateEvents() {
            return iterateEvents;
        }

        public List<TableMeta> getIterateTables() {
            return iterateTables;
        }

        public void setIterateEvents(List<LogEvent> iterateEvents) {
            this.iterateEvents = iterateEvents;
        }

        public void setIterateTables(List<TableMeta> iterateTables) {
            this.iterateTables = iterateTables;
        }

        public List<CanalEntry.Entry> getIterateEntrys() {
            return iterateEntrys;
        }

        public void setIterateEntrys(List<CanalEntry.Entry> iterateEntrys) {
            this.iterateEntrys = iterateEntrys;
        }
    }

    static class MessageEventFactory implements EventFactory<MessageEvent> {

        @Override
        public MessageEvent newInstance() {
            return new MessageEvent();
        }
    }

    public void setConnection(ErosaConnection connection) {
        this.connection = connection;
    }

    public void setGtidSet(GTIDSet gtidSet) {
        this.gtidSet = gtidSet;
    }

}
