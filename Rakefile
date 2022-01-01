require "erb"

rule '.go' => '.go.erb' do |task|
  erb = ERB.new(File.read(task.source))
  File.write(task.name, "// Do not edit. Generated from #{task.source}\n" + erb.result(binding))
  sh "goimports", "-w", task.name
end

desc "Generate code"
task generate: ["pgtype/int.go"]
