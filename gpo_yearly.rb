require 'zoom'
require 'pp'
require 'marc'
require 'json'
require 'stringio'
require 'dotenv'
require 'date'
require 'registry/registry_record'
require 'registry/source_record'
require 'objspace'

SR = Registry::SourceRecord
RR = Registry::RegistryRecord

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

today = Date.today # so we know if/when to stop
year = ARGV.shift.to_i
raise 'Bad year' unless year > 2000 && year <= today.year

d = Date.new(year,1,1)
dend = Date.new(year,12,31)
if dend > today
  dend = today
end
num_nil = 0
rrcount = 0
new_count = 0
num_updated = 0
update_count = 0

while d <= dend
  dstring = d.strftime("%Y%m%d")
  
  rset = con.search("@attr 1=1012 #{dstring}*")
  puts "num records:#{rset.size}"
 
  rset.each_record do |rec|
    r = MARC::Reader.new(StringIO.new(rec.raw), encoding_options)
    m = r.first
    if m['001'].nil?
      num_nil += 1
      next
    end
    gpo_local_id = m['001'].value.gsub(/^0+/, '')
    src = SR.where(org_code:"dgpo",
                   local_id:gpo_local_id).first
    if src
      src.source = m.to_hash.to_json
      num_updated += 1
    else
      src = SR.new(org_code:"dgpo",
                   source: m.to_hash.to_json)
      new_count += 1
    end
    # '$' has snuck into at least one 040. It's wrong and Mongo chokes on it.
    s040 = src.source['fields'].select {|f| f.keys[0] == '040'}
    if s040 != []
      s040[0]['040']['subfields'].delete_if {|sf| sf.keys[0] == '$'}
    end
    src.in_registry = true
    src.save
    res = src.add_to_registry "GPO yearly: #{year.to_s}."
    rrcount += res[:num_new]
    sleep(1)
  end
  
  if d.day == 1
    puts d
  end
  d = d.next
end

puts "gpo new regrec count: #{rrcount}"
puts "gpo new srcs: #{new_count}"
puts "gpo updated srcs: #{num_updated}"
puts "gpo recs with no 001: #{num_nil}"
