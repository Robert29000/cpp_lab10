#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <vector>
#include <boost/program_options.hpp>
#include <header.hpp>
#include <string>
#include <ThreadPool.h>
#include <picosha2.h>
#include <boost/log/trivial.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared_object.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/attributes/current_thread_id.hpp>
#include <boost/log/support/date_time.hpp>


namespace logging = boost::log;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;
namespace expr = boost::log::expressions;
namespace attrs = boost::log::attributes;
using namespace boost::program_options;
using namespace rocksdb;

void calculate_column_hash(DB* db_in, DB* db_out, ColumnFamilyHandle* handle_in);
void init_options(int argc, char* argv[], std::string& input_dir, std::string& output_dir, std::string& log_level, int& thread_n);
void init_logger(const std::string& log_level);
logging::trivial::severity_level get_enum(const std::string& log);

int main(int argc, char* argv[]) {

  std::string input_dir, output_dir, log_level;
  int thread_n;

  init_options(argc, argv,input_dir, output_dir, log_level, thread_n);
  init_logger(log_level);

  DB* db_i, *db_o;
  Options options;
  options.create_if_missing = true;
  Status s;
  std::vector<std::string> columns_name;
  std::vector<ColumnFamilyDescriptor> families;
  s = DB::ListColumnFamilies(DBOptions(), input_dir, &columns_name);
  assert(s.ok());
  for (const auto& name: columns_name){
    families.push_back(ColumnFamilyDescriptor(name, ColumnFamilyOptions()));
  }
  std::vector<ColumnFamilyHandle*> handlers_i;
  s = DB::OpenForReadOnly(DBOptions(), input_dir, families, &handlers_i, &db_i);
  assert(s.ok());

  s = DB::Open(options, output_dir, &db_o);
  assert(s.ok());

  ThreadPool pool_hash(thread_n);
  std::vector<std::future<void>> cols;
  for (auto& handler : handlers_i) {
    cols.push_back(pool_hash.enqueue(&calculate_column_hash, std::cref(db_i), std::cref(db_o), handler));
  }

  for (auto& col: cols){
    col.get();
  }

  for(auto handler: handlers_i){
    s = db_i->DestroyColumnFamilyHandle(handler);
    assert(s.ok());
  }

  delete db_i;
  delete db_o;
  return 0;
}

void calculate_column_hash(DB* db_in, DB* db_out, ColumnFamilyHandle* handle_in){
  ColumnFamilyHandle* handle_out;
  Status s;
  if (handle_in->GetName() != kDefaultColumnFamilyName) {
    s = db_out->CreateColumnFamily(ColumnFamilyOptions(),
                                          handle_in->GetName(), &handle_out);
  }
  assert(s.ok());
  Iterator* it = db_in->NewIterator(ReadOptions(), handle_in);
  WriteBatch batch;
  for (it->SeekToFirst(); it->Valid(); it->Next()){
    std::string hash;
    auto key = it->key().ToString();
    auto value = it->value().ToString();
    picosha2::hash256_hex_string(value,hash);
    batch.Put(handle_out, Slice(key), Slice(hash));
    BOOST_LOG_TRIVIAL(info) << "column: " << handle_in->GetName() << " key: " << key << " value: " << value << " hash: " << hash;
  }
  s = db_out->Write(WriteOptions(), &batch);
  assert(s.ok());
  assert(it->status().ok());
  delete it;

  s = db_out->DestroyColumnFamilyHandle(handle_out);
  assert(s.ok());
}

void init_options(int argc, char* argv[], std::string& input_dir, std::string& output_dir, std::string& log_level, int& thread_n){
  options_description desc{"Options"};
  desc.add_options()("help,h", "Show help")
      ("help,h", "Show help")
      ("log_level,l",value<std::string>(&log_level)->default_value("error"), "set level of logging")
      ("thread-count,t", value<int>(&thread_n)->default_value(2), "set number of threads")
      ("input,i", value<std::string>(&input_dir), "path to input file")
      ("output,o", value<std::string>(&output_dir)->default_value(input_dir), "path to store output file");
  positional_options_description pos_desc;
  pos_desc.add("input", -1);

  command_line_parser parser{argc, argv};
  parser.options(desc).positional(pos_desc).allow_unregistered();
  parsed_options parsed_options = parser.run();

  variables_map vm;
  store(parsed_options, vm);
  notify(vm);
}

void init_logger(const std::string& log_level){
  boost::log::add_common_attributes();
  boost::shared_ptr<logging::core> core = logging::core::get();
  core->add_global_attribute("ThreadID", attrs::current_thread_id());
  boost::shared_ptr<sinks::text_file_backend > backend =
      boost::make_shared<sinks::text_file_backend>(
          keywords::file_name = "logs/file_%5N.log",
          keywords::rotation_size = 5 * 1024 * 1024);

  typedef sinks::synchronous_sink
      <sinks::text_file_backend> sink_f; // file sink
  boost::shared_ptr<sink_f> file_sink(new sink_f(backend));
  file_sink->set_formatter(expr::stream
                               << expr::format_date_time<boost::posix_time::ptime>
                                   ("TimeStamp", "%Y-%m-%d %H:%M:%S")
                               << ": <" << logging::trivial::severity
                               << "> <"  << expr::attr<attrs::current_thread_id::value_type>("ThreadID")
                               << "> " << expr::smessage);

  file_sink->set_filter(logging::trivial::severity >= get_enum(log_level));
  core->add_sink(file_sink);
}

logging::trivial::severity_level get_enum(const std::string& log){
  using namespace logging::trivial;
  if (log == "error"){
    return severity_level::error;
  } else if (log == "info"){
    return severity_level::info;
  } else {
    return severity_level::warning;
  }
}