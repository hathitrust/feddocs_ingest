# update preexisting HathiTrust records
# add new HathiTrust records existing Registry Records
# create new Registry Records if no match can be found
# we are NOT recalculating clustering!!!
#
require 'registry_record'
require 'source_record'
require 'normalize'
require 'json'
require 'dotenv'
require 'pp'

begin
Mongoid.load!(File.expand_path("../config/mongoid.yml", __FILE__), :development)

ORGCODE = 'miaahdl'
OCLCPAT = 
  /
      \A\s*
      (?:(?:\(OCo?LC\)) |
  (?:\(OCo?LC\))?(?:(?:ocm)|(?:ocn)|(?:on))
  )(\d+)
  /x

def has_sudoc marc
  field_086 = marc['fields'].find {|f| f['086'] }
  if field_086 and 
    (field_086['086']['ind1'] == '0' or field_086['086']['a'] =~ /:/)
    return true 
  else
    return false
  end
end


fin = ARGV.shift
puts fin
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

  new_src = SourceRecord.new
  new_src.org_code = "miaahdl"
  new_src.source = line
  new_src.local_id = new_src.extract_local_id

  marc = JSON.parse line

  field_008 = marc['fields'].find {|f| f['008'] }['008']

     
  # pre-existing source record that has been updated
  src = SourceRecord.where(org_code: ORGCODE, local_id: new_src.local_id).first
  if src
    src_count[src.source_id] = 0
    #new enum chrons means new or updated regrec
    new_enum_chrons = new_src.enum_chrons - src.enum_chrons

    #trust that it's an improvement
    src.source = line
    src.source_blob = line
    src.in_registry = true
    src.save
    #puts src.source_id    
    update_count += 1

    if new_enum_chrons 
      new_enum_chrons.each do  |ec| 
        if regrec = RegistryRecord::cluster( src, ec)
          regrec.add_source(src)
        else
          regrec = RegistryRecord.new([src.source_id], ec, "HT update: #{fin}")
        end
        regrec.save
        rr_ids << regrec.registry_id
      end
    end
    src.enum_chrons = new_src.enum_chrons
    src.save
    RegistryRecord.where(source_record_ids:src.source_id).no_timeout.each do |rr| 
      #rr.recollate #this blows up when dealing Serial Set or CFR
      rr.save
      rrcount += 1
      src_count[src.source_id] += 1
      rr_ids << rr.registry_id
    end
  #new source record
  elsif field_008 =~ /^.{17}u.{10}f/ or 
        (new_src.oclc_resolved.count > 0 and SourceRecord.in(oclc_resolved:new_src.oclc_resolved).first) or
        has_sudoc(marc)
    new_src.source_blob = line
    new_src.in_registry = true
    new_src.save
    #puts "new source: #{htid}"
    new_count += 1

    src_count[new_src.source_id] = 0

    if new_src.enum_chrons == []
      new_src.enum_chrons << ""
    end


    new_src.enum_chrons.each do |ec| 
      if regrec = RegistryRecord::cluster( new_src, ec)
        regrec.add_source(new_src)
      else
        regrec = RegistryRecord.new([new_src.source_id], ec, "HT update: #{fin}")
      end
      regrec.save
      rrcount += 1
      src_count[new_src.source_id] += 1
      #rr_ids << regrec.registry_id
    end
  #not an update or a new gov doc record
  else
    next
  end

end
puts "regrec count: #{rrcount}"
puts "new srcs: #{new_count}"
puts "updates: #{update_count}"
#PP.pp src_count 

rescue Exception => e
  PP.pp e
  puts e.backtrace
end

