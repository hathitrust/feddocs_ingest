# Harvard provides their metadata here: http://library.harvard.edu/open-metadata
#
# They claim updates are made weekly, but monthly or quarterly should be adequate
# for our needs. 
#
# Take a bunch of Harvard's .mrc files. 
# Identify records for govdocs and insert/update as appropriate
#
# Only using monographs, because there aren't any holdings data available
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

ORGCODE = "mh"

#each .mrc file
ARGV.each do | infile |
  puts infile
  reader = MARC::Reader.new(infile, encoding_options)
  for record in reader
    total += 1
    
    # silly, but we usually expect json
    line = record.to_hash.to_json

    @new_src = SourceRecord.new
    @new_src.org_code = ORGCODE 
    @new_src.source = line 

    if !@new_src.is_monograph? or !@new_src.is_govdoc
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
      src.save
      res = src.update_in_registry "MH update: #{infile}"
      num_new_rr += res[:num_new] 

    #new source record
    elsif 
      num_govdocs += 1
      @new_src.in_registry = true
      @new_src.save
      num_new_bib += 1
      res = @new_src.add_to_registry "MH update: #{infile}"
      num_new_rr += res[:num_new]
    end 
  end # each record
end # each .mrc file

puts "# of Govdoc records: #{num_govdocs}"
puts "# of new Registry Records: #{num_new_rr}"
puts "# of new Govdoc bib records: #{num_new_bib}"

rescue Exception => e
  PP.pp e
  puts e.backtrace
  PP.pp @new_src
  PP.pp @new_source.source
end

