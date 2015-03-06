local ctr = require 'box.conntrack'

local M = setmetatable({
}, {  })

function M:register(ref, cb)
	ctr.on_disconnect(ref,cb)
end

return M
