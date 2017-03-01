# Take a bunch of Michigan's .xml files. 
# Identify records for govdocs and insert/update as appropriate
#
require 'registry/registry_record'
require 'registry/source_record'
require 'marc'
require 'json'
require 'dotenv'

SourceRecord = Registry::SourceRecord
RegistryRecord = Registry::RegistryRecord
begin
Dotenv.load
Mongoid.load!("config/mongoid.yml", :development)
Mongo::Logger.logger.level = ::Logger::FATAL

encoding_options = { 
  :external_encoding => "UTF-8",
  :invalid => :replace,
  :undef   => :replace,
  :replace => '', 
}
total = 0
num_govdocs = 0
num_new_rr = 0
num_new_bib = 0
@new_src

ORGCODE = "miu"

#each .xml file
ARGV.each do | infile |
  puts infile
  reader = MARC::XMLReader.new(infile, encoding_options)
  for record in reader
    total += 1
    
    # silly, but we usually expect json
    line = record.to_hash.to_json

    @new_src = SourceRecord.new
    @new_src.org_code = ORGCODE 
    @new_src.source = line 

    if !@new_src.is_govdoc
      puts line
      next
    end
    
    @new_src.local_id = @new_src.extract_local_id

    if @new_src.source.nil?
      next
    end

    # pre-existing source record that has been updated
    src = SourceRecord.where(org_code: ORGCODE, local_id: @new_src.local_id).first
    if src
      num_govdocs += 1
      src.source = line
      src.source_blob = line
      src.in_registry = true   
      src.save
      res = src.add_to_registry "UMich update. 2017-03-01"
      num_new_rr += res[:num_new]
    #new source record
    elsif 
      @new_src.in_registry = true
      @new_src.save
      res = @new_src.add_to_registry "UMich update. 2017-03-01"
      num_new_rr += res[:num_new]
      num_govdocs += 1
      num_new_bib += 1
    end 
    
  end # each record
end # each .mrc file

puts "# of Govdoc records: #{num_govdocs}"
puts "# of new Govdoc bib records: #{num_new_bib}"
puts "# of records: #{total}"
puts "# of new Reg Recs: #{num_new_rr}"

rescue Exception => e
  PP.pp e
  puts e.backtrace
  PP.pp @new_src
  PP.pp @new_source.source
end

