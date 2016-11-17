# run very occasionally.
# Requests every GPO record sequentially to fill in missing/incomplete records
#
require 'zoom'
require 'pp'
require 'marc'
require 'json'
require 'stringio'
require 'registry/registry_record'
require 'registry/source_record'
require 'dotenv'
SourceRecord = Registry::SourceRecord
RegistryRecord = Registry::RegistryRecord

Dotenv.load!

Mongoid.load!(File.expand_path("../config/mongoid.yml", __FILE__), :development)

con = ZOOM::Connection.new()
con.user = 'z39'
con.password = ENV["z3950_password"] 
con.database_name = 'gpo01pub'
con.preferred_record_syntax = 'USMARC'
con.connect('z3950.catalog.gpo.gov', 9991)

encoding_options = {
  :external_encoding => "MARC-8",
  :invalid => :replace,
  :undef   => :replace,
  :replace => '',
}
rrcount = 0
new_count = 0
update_count = 0
nil_count = 0
bad_001_count = 0
rr_update_count = 0

#1. get highest existing GPO id 
# Unlike the weekly update, we will stop here. 
highest_id = SourceRecord.where(org_code:"dgpo").max(:local_id)
puts "highest id: #{highest_id}"

#2. ask for recs by id until we get to the highest id 
(1..50).each do |current_id|
  sleep(.3) #be polite
  
  rset = con.search("@attr 1=12 #{current_id}")
  if !rset[0]
    nil_count += 1
    puts "nil at #{current_id}"
    next
  end

  r = MARC::Reader.new(StringIO.new(rset[0].raw), encoding_options)
  marc = r.first
  if marc['001'].nil?
    bad_001_count += 1
    next
  end

  gpo_id = marc['001'].value.to_i
  line = marc.to_hash.to_json #round about way of doing things 
  src = SourceRecord.where(org_code:"dgpo", local_id:gpo_id).first
  if src.nil?
    src = SourceRecord.new
    new_count += 1
    src.org_code = "dgpo"
    src.local_id = gpo_id 
  else
    update_count += 1
  end
  src.source = line
  src.source_blob = line
  if src.enum_chrons == []
    src.enum_chrons << ""
  end

  # '$' has snuck into at least one 040. It's wrong and Mongo chokes on it.
  s040 = src.source['fields'].select {|f| f.keys[0] == '040'}
  if s040 != []
    s040[0]['040']['subfields'].delete_if {|sf| sf.keys[0] == '$'}
  end
  
  src.in_registry = true
  src.save

  #3. cluster/create regrecs
  src.enum_chrons.each do |ec| 
    if regrec = RegistryRecord::cluster( src, ec)
      regrec.add_source(src)
      rr_update_count += 1
    else
      regrec = RegistryRecord.new([src.source_id], ec, "GPO #{Date.today}")
      rrcount += 1
    end
     
    #GPO does something dumb with DNA
    if !regrec.subject_t.nil? and regrec.subject_t.include? "Norske arbeiderparti."
      regrec.subject_t.delete("Norske arbeiderparti.")
      if !regrec.subject_t.include? "DNA"
        regrec.subject_t << "DNA"
      end
      regrec.subject_topic_facet.delete("Norske arbeiderparti.")
      if !regrec.subject_topic_facet.include? "DNA"
        regrec.subject_topic_facet << "DNA"
      end
    end

    regrec.save
  end
end

puts "gpo new regrec count: #{rrcount}"
puts "gpo updated regrec count: #{rr_update_count}"
puts "gpo new srcs: #{new_count}"
puts "gpo source updates: #{update_count}"
puts "nil count: #{nil_count}"
puts "bad 001 count: #{bad_001_count}"

