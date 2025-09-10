#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>
#include <sys/resource.h>
#include "Poco/Util/ServerApplication.h"
#include "Daemon/BaseDaemon.h"
#include "AggregateFunctions/registerAggregateFunctions.h"
#include "Client/ClientApplicationBase.h"
#include "Columns/ColumnArray.h"
#include "Columns/ColumnString.h"
#include "Databases/registerDatabases.h"
#include "Dictionaries/registerDictionaries.h"
#include "Disks/registerDisks.h"
#include "Formats/registerFormats.h"
#include "Functions/registerFunctions.h"
#include "Interpreters/ClusterProxy/executeQuery.h"
#include "Interpreters/Context.h"
#include "Interpreters/executeQuery.h"
#include "Interpreters/registerInterpreters.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/Kusto/KustoFunctions/KQLDataTypeFunctions.h"
#include "Parsers/ParserCreateQuery.h"
#include "Parsers/ParserSelectQuery.h"
#include "Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h"
#include "Processors/QueryPlan/QueryPlan.h"
#include "Processors/Transforms/DeduplicationTokenTransforms.h"
#include "Storages/MergeTree/MergeTreeSink.h"
#include "Storages/StorageFactory.h"
#include "Storages/registerStorages.h"
#include "TableFunctions/registerTableFunctions.h"

#include "config.h"

#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wmissing-declarations"

struct MergeTreeAccessor : public Poco::Util::Application
{
    void initialize(Poco::Util::Application & self) override
    {
        Poco::Util::Application::initialize(self);

        DB::registerInterpreters();
        /// Don't initialize DateLUT
        DB::registerFunctions();
        DB::registerAggregateFunctions();
        DB::registerTableFunctions();
        DB::registerDatabases();
        DB::registerStorages();
        DB::registerDictionaries();
        DB::registerDisks(/* global_skip_access_check= */ true);
        DB::registerFormats();

        shared_context = DB::Context::createShared();
        global_context = DB::Context::createGlobal(shared_context.get());
        global_context->makeGlobalContext();
        global_context->setPath("./db");

        GlobalThreadPool::initialize();
        DB::getIOThreadPool().initialize(10, 1, 100);
        DB::getActivePartsLoadingThreadPool().initialize(10, 1, 100);
        DB::getOutdatedPartsLoadingThreadPool().initialize(10, 1, 100);
        DB::getUnexpectedPartsLoadingThreadPool().initialize(10, 1, 100);
        DB::getPartsCleaningThreadPool().initialize(10, 1, 100);

        DB::getDatabaseCatalogDropTablesThreadPool().initialize(10, 1, 100);
        DB::getMergeTreePrefixesDeserializationThreadPool().initialize(10, 1, 100);
        DB::getFormatParsingThreadPool().initialize(10, 1, 100);
    }

    int main(const std::vector<std::string> & /*args*/) override
    {
        // parseCreateQuery();
        // createAndWriteToMergeTree();
        attachAndSelect();

        return 0;
    }

    void parseCreateQuery()
    {
        std::string query_txt = "CREATE TABLE test_table (key UInt64, value String) ENGINE = MergeTree ORDER BY key";

        DB::ParserCreateQuery create_parser;
        DB::ASTPtr query = DB::parseQuery(create_parser, query_txt, 0, DB::DBMS_DEFAULT_MAX_PARSER_DEPTH, DB::DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        DB::ASTCreateQuery & create_query = query->as<DB::ASTCreateQuery &>();

        DB::ColumnsDescription columns;
        columns.add(DB::ColumnDescription("key", std::make_shared<DB::DataTypeUInt64>()));
        columns.add(DB::ColumnDescription("value", std::make_shared<DB::DataTypeString>()));

        DB::ConstraintsDescription constraints;

        auto storagePtr = DB::StorageFactory::instance().get(create_query,
            "./db/",
            global_context,
            global_context,
            columns,
            constraints,
            DB::LoadingStrictnessLevel::CREATE);
    }

    void attachAndSelect()
    {
        auto metadata = std::make_shared<DB::StorageInMemoryMetadata>();
        metadata->columns.add(DB::ColumnDescription("key", std::make_shared<DB::DataTypeUInt64>()));
        metadata->columns.add(DB::ColumnDescription("value", std::make_shared<DB::DataTypeString>()));

        auto astIdentifier = std::make_shared<DB::ASTIdentifier>("key");

        metadata->partition_key = DB::KeyDescription::buildEmptyKey();
        auto keyDefinition = DB::KeyDescription::getSortingKeyFromAST(astIdentifier, metadata->columns, global_context, std::nullopt);
        metadata->sorting_key = keyDefinition;
        metadata->primary_key = keyDefinition;

        DB::String date_column_name {};
        DB::MergeTreeData::MergingParams merging_params;
        merging_params.mode = DB::MergeTreeData::MergingParams::Ordinary;
        auto merge_tree_settings = std::make_unique<DB::MergeTreeSettings>();

        auto tree = std::make_shared<DB::StorageMergeTree>(
            id,
            path,
            *metadata,
            DB::LoadingStrictnessLevel::ATTACH, global_context, date_column_name, merging_params, std::move(merge_tree_settings));

        tree->startup();

        auto query_context = DB::Context::createCopy(global_context);
        query_context->makeQueryContext();

        DB::QueryPlan plan;
        auto storage_snapshot_ = tree->getStorageSnapshotForQuery(tree->getInMemoryMetadataPtr(), nullptr, query_context);
        DB::SelectQueryInfo query_info;
        query_info.query = std::make_shared<DB::ASTSelectQuery>();

        tree->read(plan,
            { "key", "value" },
            storage_snapshot_,
            query_info,
            query_context,
            DB::QueryProcessingStage::FetchColumns,
            1024,
            10);

        if (plan.isInitialized())
        {
            auto builder = plan.buildQueryPipeline(DB::QueryPlanOptimizationSettings(query_context), DB::BuildQueryPipelineSettings(query_context));
            DB::QueryPlanResourceHolder resources;
            auto pipe = DB::QueryPipelineBuilder::getPipe(std::move(*builder), resources);
            auto query_pipeline = DB::QueryPipeline(std::move(pipe));
            auto executor = std::make_unique<DB::PullingPipelineExecutor>(query_pipeline);
            DB::Chunk chunk;

            while (executor && executor->pull(chunk))
            {
                std::cout << "Chunk: " << chunk.getNumColumns() << " | " << chunk.getNumRows() << std::endl;
                auto columns = chunk.getColumns();
                for (std::uint64_t row = 0; row < chunk.getNumRows(); row++)
                {
                    for (std::uint64_t col = 0; col < chunk.getNumColumns(); col++)
                    {
                        auto& column = columns[col];
                        switch (col)
                        {
                            case 0: {
                                std::cout << column->get64(row) << " ";
                                break;
                            }
                            case 1: {
                                DB::Field f;
                                column->get(row, f);
                                std::cout << f.safeGet<DB::String>() << std::endl;
                                break;
                            }
                            default: {
                                std::terminate();
                            }
                        }
                    }
                }
            }
        }

        // std::string query_txt = "SELECT * FROM test_table";
        //
        // DB::ParserSelectQuery select_parser;
        // DB::ASTPtr query = DB::parseQuery(select_parser, query_txt, 1000, DB::DBMS_DEFAULT_MAX_PARSER_DEPTH, DB::DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        //
        // DB::ASTSelectQuery & select_query = query->as<DB::ASTSelectQuery &>();
        //
        // auto query_context = DB::Context::createCopy(global_context);
        // query_context->makeQueryContext();
        // auto io = DB::executeQuery(query_txt, query_context, DB::QueryFlags{ .internal = true }).second;
        // // DB::PullingPipelineExecutor executor(io.pipeline);
        // DB::Block res;
        // while (res.empty() && executor.pull(res));
        //
        // DB::Block tmp_block;
        // while (executor.pull(tmp_block));
        // (void)select_query;
    }

    void createAndWriteToMergeTree()
    {
        auto metadata = std::make_shared<DB::StorageInMemoryMetadata>();
        metadata->columns.add(DB::ColumnDescription("key", std::make_shared<DB::DataTypeUInt64>()));
        metadata->columns.add(DB::ColumnDescription("value", std::make_shared<DB::DataTypeString>()));

        auto astIdentifier = std::make_shared<DB::ASTIdentifier>("key");

        metadata->partition_key = DB::KeyDescription::buildEmptyKey();
        auto keyDefinition = DB::KeyDescription::getSortingKeyFromAST(astIdentifier, metadata->columns, global_context, std::nullopt);
        metadata->sorting_key = keyDefinition;
        metadata->primary_key = keyDefinition;

        DB::String date_column_name {};
        DB::MergeTreeData::MergingParams merging_params;
        merging_params.mode = DB::MergeTreeData::MergingParams::Ordinary;
        auto merge_tree_settings = std::make_unique<DB::MergeTreeSettings>();
        auto tree = std::make_shared<DB::StorageMergeTree>(
            id,
            path,
            *metadata,
            DB::LoadingStrictnessLevel::CREATE, global_context, date_column_name, merging_params, std::move(merge_tree_settings));

        tree->startup();

        DB::MergeTreeSink sink(*tree, metadata, 10, global_context);
        DB::Chunk chunk;

        auto intCol = DB::ColumnUInt64::create();
        auto strCol = DB::ColumnString::create();

        for (std::size_t i = 1; i < 11; ++i)
        {
            intCol->insertValue(i);
            strCol->insert(DB::Field("string value " + std::to_string(i)));
        }

        DB::Columns columns;
        columns.push_back(intCol->getPtr());
        columns.push_back(strCol->getPtr());

        chunk.setColumns(columns, 10);
        chunk.getChunkInfos().add(std::make_shared<DB::DeduplicationToken::TokenInfo>());

        sink.onStart();
        sink.consume(chunk);
        sink.onFinish();


    }

private:
    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr global_context;

    DB::StorageID id { "test_db", "test_table" };
    DB::String path { "./" };
};

int mainEntryClickHousePoC(int /*argc*/, char ** /*argv*/) {
    DB::MainThreadStatus::getInstance();

    try
    {
        MergeTreeAccessor app;
        return app.run();
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        return DB::getCurrentExceptionCode();
    }
}
