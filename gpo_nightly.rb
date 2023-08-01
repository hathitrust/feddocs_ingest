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

today = Date.today
yesterday = today.prev_day

dstring = yesterday.strftime("%Y%m%d")
puts dstring

rset = con.search("@attr 1=1012 #{dstring}*")
puts "num records:#{rset.size}"

if ARGV.include?("--test_connection")
  raise "only testing connection" # no further processing
end

rrcount = 0
new_count = 0
num_updated = 0
update_count = 0
num_nil = 0

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
  #puts gpo_local_id
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
  res = src.add_to_registry "GPO nightly."
  rrcount += res[:num_new]
  sleep(2)
end

puts "gpo new regrec count: #{rrcount}"
puts "gpo new srcs: #{new_count}"
puts "gpo updated srcs: #{num_updated}"
puts "gpo recs with no 001: #{num_nil}"
