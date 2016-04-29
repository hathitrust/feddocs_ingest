# Add GPO records 
# cluster with existing Registry Records
# create new Registry Records if no match can be found
# 
# run once. use this as lessons for weekly update
#
require 'registry_record'
require 'source_record'
require 'normalize'
require 'json'
require 'dotenv'
require 'pp'

begin
Mongoid.load!(File.expand_path("../config/mongoid.yml", __FILE__), :development)

ORGCODE = 'dgpo'

fin = ARGV.shift
if fin =~ /\.gz$/
  updates = Zlib::GzipReader.open(fin)
else
  updates = open(fin)
end

new_count = 0
update_count = 0
src_count = {}
rr_ids = []
count = 0
rrcount = 0
updates.each do | line | 
  count += 1 

  line.chomp!
  marc = JSON.parse line

  #records without 001 are junk
  gpo_id = marc['fields'].find {|f| f['001'] }
  if gpo_id.nil?
    next
  else
    gpo_id = gpo_id['001'].to_i
  end

  enum_chrons = []

  src = SourceRecord.new
  src.source = line
  src.source_blob = line
  src.org_code = ORGCODE
  src.local_id = src.extract_local_id.to_i
  enum_chrons = src.extract_enum_chrons
  src.enum_chrons = enum_chrons.flatten.uniq
  src.in_registry = true
  src.save
  new_count += 1

  if src.enum_chrons == []
    src.enum_chrons << ""
  end


  src.enum_chrons.each do |ec| 
    if regrec = RegistryRecord::cluster( src, ec)
      regrec.add_source(src)
      update_count += 1
    else
      regrec = RegistryRecord.new([src.source_id], ec, "GPO initial load")
      rrcount += 1
    end
    regrec.save
    rr_ids << regrec.registry_id
  end

end
puts "gpo new regrec count: #{rrcount}"
puts "gpo new srcs: #{new_count}"
puts "gpo regrec updates: #{update_count}"

rescue Exception => e
  PP.pp e
  puts e.backtrace
end

