# Don't use this. Use the gpo_nightly or gpo_yearly
#
# This pulls based on local id number which is a terrible way of doing it



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

Mongoid.load!(ENV['MONGOID_CONF'], :production)

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

#1. Set our start at 0. We won't request what we already have
nil_count = 0
current_id = 0 

#1. get highest existing GPO id 
highest_id = SourceRecord.where(org_code:"dgpo").max(:local_id).to_i
puts "highest id: #{highest_id}"
current_id = highest_id - 1000

#2. ask for recs by id until we get too many consecutive nils 
while nil_count < 20 do #arbitrary
  current_id += 1

  src_count = SourceRecord.where(org_code:"dgpo",
                           local_id: current_id.to_s,
                           deprecated_timestamp:{"$exists":0}).no_timeout.count
  if src_count != 0
    nil_count = 0 # reset, looking for consecutive
    next
  end
  
  sleep(5) #be polite
  
  rset = con.search("@attr 1=12 #{current_id}")
  if !rset[0]
    nil_count += 1
    puts "nil at #{current_id}"
    next
  else
    nil_count = 0 #reset, looking for consecutive
  end

  r = MARC::Reader.new(StringIO.new(rset[0].raw), encoding_options)
  marc = r.first
  if marc['001'].nil?
    next
  end

  gpo_id = marc['001'].value.gsub(/^0+/, '')
  line = marc.to_hash.to_json #round about way of doing things 
  src = SourceRecord.where(org_code:"dgpo", 
                           local_id:gpo_id).first # obv this shouldn't exist
  src ||= SourceRecord.new
  new_count += 1
  src.org_code = "dgpo"
  src.source = line
  # '$' has snuck into at least one 040. It's wrong and Mongo chokes on it.
  s040 = src.source['fields'].select {|f| f.keys[0] == '040'}
  if s040 != []
    s040[0]['040']['subfields'].delete_if {|sf| sf.keys[0] == '$'}
  end
  src.in_registry = true
  src.save
  res = src.add_to_registry "GPO weekly."
  rrcount += res[:num_new]

end

puts "gpo new regrec count: #{rrcount}"
puts "gpo new srcs: #{new_count}"
#puts "gpo regrec updates: #{update_count}"

