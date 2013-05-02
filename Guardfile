# A sample Guardfile
# More info at https://github.com/guard/guard#readme

context = Struct.new(:path) # default
=begin Context Object
context = Class.new do
  def initialize(path)
  end
  # custom behaviour
end
=end

locals = Hash.new({}) # default
=begin Local Variables
require 'yaml'
locals, path = {}, 'locals.yml'
def locals.reload
  update YAML.load_file(path)
end
locals.reload
=end

guard 'tilt', :context => context, :locals => locals do
  # watch files with two extnames like index.html.erb
  watch %r'.+go.erb'
end

# Guard::Tilt.root = Dir.getwd # (default: Dir.getwd)

=begin Output Path
class OuputPath < Guard::Tilt::OutputPath

  BASE = File.expand_path 'views'
  ROOT = File.expand_path 'public'

  # By default Path#sanitize only strips an extname from itself.
  #
  # If you want to write the rendered output to another folder you can
  # overwrite this method to return another Path object, like this:
  def sanitize
    super.sub BASE, ROOT
  end

  # ... then set the OutputPath class.
  Guard::Tilt.output_path = self

end
=end
