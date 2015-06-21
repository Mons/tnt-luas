local Date = {}
Date.__index = Date
Date.__name = 'Date'

local int = math.floor
local min = math.min
local tz = 0

local function jd (y, m, d)
	local a = int((14 - m) / 12)
	y = y + 4800 - a
	m = m + 12 * a - 3
	return d+int((153*m+2)/5)+365*y+int(y/4)-int(y/100)+int(y/400)-32045
end

function Date.midnight_offset(...)
	if ... then
		tz = ...
	end
	return tz
end

function Date.zzz() return tz end

function Date:_sync_jd()
	self.jd = jd(self.year, self.month, self.day)
	return self
end

function Date:_sync_ymd()
	local f = self.jd + 1401 + int (int((4 * self.jd + 274277) / 146097) * 3 / 4) - 38
	local e = 4 * f + 3
	local g = int (e % 1461 / 4)
	local h = 5 * g + 2
	local ret = {}
	self.day = int (h % 153 / 5) + 1
	self.month = int (h / 153 + 2) % 12 + 1
	self.year  = int (e / 1461) - 4716 + int ((14 - self.month) / 12)
	return self
end

function Date:days_in_month(y, m)
	if m == 2 then
		return ((y % 4 == 0 and (y % 100 ~= 0 or y % 100 == 0)) and 29 or 28)
	else
		return (((m % 2 == 1 and m < 8) or (m % 2 == 0 and m > 7)) and 31 or 30)
	end
end

function Date.fromymd(class, y, m, d)
	local self = setmetatable({}, class.__index)
	self.year = y
	self.month = m
	self.day = d
	self:_sync_jd()
	return self
end

Date.new = Date.fromymd
setmetatable(Date, { __call = Date.new })

function Date.fromjd(class, jd)
	local self = setmetatable({ ["jd"] = jd }, class.__index)
	self:_sync_ymd()
	return self
end

function Date:clone()
	return setmetatable({
		year = self.year,
		month = self.month,
		day = self.day,
		jd = self.jd
	}, self.__index)
end

function Date.fromunix(class, ts)
	--if(type(ts) ~= 'number') then print ('======') print(debug.traceback()) end
	local jd = int ((ts + tz)/ 86400) + 2440588
	return class:fromjd(jd)
end

function Date:unix()
	return (self.jd - 2440588) * 86400 - tz
end

function Date:today()
	return self:fromunix(os.time())
end

function Date:tomorrow()
	return self:fromunix(os.time() + 86400)
end

function Date:yesterday()
	return self:fromunix(os.time() - 86400)
end

function Date:add_months(months)
	self.year = self.year + int((self.month + months - 1) / 12)
	self.month = (self.month + months - 1) % 12 + 1
	self.day = min(self:days_in_month(self.year, self.month), self.day)
	self:_sync_jd()
	return self
end

function Date:day_restricted(day)
	self.day = min(self:days_in_month(self.year, self.month), day)
	self:_sync_jd()
	return self
end

function Date:_copy(d)
	self.year = d.year
	self.month = d.month
	self.day = d.day
	self.jd = d.jd
	return self
end

function Date:add_days(days)
	self:_copy(self:fromjd(self.jd + days))
	return self
end

function Date:delta_days(d)
	return d.jd - self.jd
end

function Date:__tostring()
	return string.format("%04d-%02d-%02d", self.year, self.month, self.day)
end

function Date:format(format)
	if type(format) == 'string' then
		return string.format(format, self.year, self.month, self.day)
	end
	return self:__tostring()
end

return Date
