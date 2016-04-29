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

  #find oclc
  oclcs = []
  marc['fields'].select {|f| f['035']}.each do | f |
    oclcs << f['035']['subfields'].select {|sf| OCLCPAT.match(sf['a'])}.collect{|o| $1.to_i}
  end
  oclcs = oclcs.flatten.uniq
  
  #is this an edit, do we already have it?
  htid = marc['fields'].find {|f| f['001'] }['001']
  field_008 = marc['fields'].find {|f| f['008'] }['008']


  enum_chrons = []
  marc['fields'].select {|f| f['974']}.each do | f | 
    enum_chrons << f['974']['subfields'].select {|sf| sf['z']}
                    .collect { |z| Normalize.enum_chron(z['z']) }
  end
  enum_chrons = enum_chrons.flatten.uniq

  # pre-existing source record that has been updated
  src = SourceRecord.where(org_code: ORGCODE, local_id: htid).first
  if src
    src_count[src.source_id] = 0
    #new enum chrons means new or updated regrec
    new_enum_chrons = enum_chrons - src.enum_chrons

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
    src.enum_chrons = enum_chrons
    src.save
    RegistryRecord.where(source_record_ids:src.source_id).no_timeout.each do |rr| 
      #rr.recollate #this blows up when dealing Serial Set or CFR
      rr.save
      src_count[src.source_id] += 1
      rr_ids << rr.registry_id
    end
  #new source record
  elsif field_008 =~ /^.{17}u.{10}f/ or 
        (oclcs.count > 0 and SourceRecord.in(oclc_resolved:oclcs).first) or
        has_sudoc(marc)
    src = SourceRecord.new
    src.source = line
    src.source_blob = line
    src.org_code = ORGCODE
    src.local_id = htid
    src.enum_chrons = enum_chrons
    src.in_registry = true
    src.save
    puts "new source: #{htid}"
    new_count += 1

    src_count[src.source_id] = 0

    if enum_chrons == []
      enum_chrons << ""
    end


    enum_chrons.each do |ec| 
      if regrec = RegistryRecord::cluster( src, ec)
        regrec.add_source(src)
      else
        regrec = RegistryRecord.new([src.source_id], ec, "HT update: #{fin}")
      end
      regrec.save
      src_count[src.source_id] += 1
      rr_ids << regrec.registry_id
    end
  #not an update or a new gov doc record
  else
    next
  end

end
puts "regrec count: #{rrcount}"
puts "new srcs: #{new_count}"
puts "updates: #{update_count}"
PP.pp src_count 

rescue Exception => e
  PP.pp e
  puts e.backtrace
end

