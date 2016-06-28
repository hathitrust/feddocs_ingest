require 'zoom'
require 'pp'
require 'marc'
require 'json'
require 'stringio'
require 'source_record'
require 'registry_record'
require 'dotenv'

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

#1. get highest existing GPO id 
highest_id = SourceRecord.where(org_code:"dgpo").max(:local_id)
puts "highest id: #{highest_id}"

#2. ask for recs by id until we get too many consecutive nils 
nil_count = 0
current_id = highest_id.to_i
while nil_count < 3 do #arbitrary
  sleep(0.5) #be polite
  current_id += 1
  
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

  gpo_id = marc['001'].value.to_i

  line = marc.to_hash.to_json #round about way of doing things 
  src = SourceRecord.new
  new_count += 1
  src.source = line
  src.source_blob = line
  src.org_code = "dgpo"
  src.local_id = gpo_id 
  if src.enum_chrons == []
    src.enum_chrons << ""
  end

  src.in_registry = true
  src.save

  #3. cluster/create regrecs
  src.enum_chrons.each do |ec| 
    if regrec = RegistryRecord::cluster( src, ec)
      regrec.add_source(src)
      update_count += 1
    else
      regrec = RegistryRecord.new([src.source_id], ec, "GPO weekly")
      rrcount += 1
    end
    regrec.save
  end

end

puts "gpo new regrec count: #{rrcount}"
puts "gpo new srcs: #{new_count}"
puts "gpo regrec updates: #{update_count}"

