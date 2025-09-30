import React, { useState, useMemo } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { Calendar, Heart, Activity, Droplet, User, ChevronLeft, ChevronRight, Download } from 'lucide-react';

// Parse dữ liệu từ query
const rawData = [
  { time: '2002-07-15 10:03:00', systolic: 120, diastolic: 75, mean: 90 },
  { time: '2002-07-15 10:04:00', systolic: 108, diastolic: 70, mean: 84 },
  { time: '2002-07-15 10:07:00', systolic: 123, diastolic: 71, mean: 90 },
  { time: '2002-07-15 10:10:00', systolic: 98, diastolic: 69, mean: 79 },
  { time: '2002-07-15 10:13:00', systolic: 97, diastolic: 69, mean: 80 },
  { time: '2002-07-15 10:17:00', systolic: 117, diastolic: 68, mean: 83 },
  { time: '2002-07-15 10:20:00', systolic: 121, diastolic: 73, mean: 88 },
  { time: '2002-07-15 10:26:00', systolic: 106, diastolic: 63, mean: 76 },
  { time: '2002-07-15 10:32:00', systolic: 108, diastolic: 65, mean: 78 },
  { time: '2002-07-15 10:38:00', systolic: 107, diastolic: 62, mean: 76 },
  { time: '2002-07-15 10:45:00', systolic: 103, diastolic: 60, mean: 73 },
  { time: '2002-07-15 10:51:00', systolic: 100, diastolic: 58, mean: 71 },
  { time: '2002-07-15 10:57:00', systolic: 92, diastolic: 56, mean: 68 },
  { time: '2002-07-15 11:10:00', systolic: 101, diastolic: 64, mean: 76 },
  { time: '2002-07-15 11:22:00', systolic: 109, diastolic: 66, mean: 81 },
  { time: '2002-07-15 11:35:00', systolic: 99, diastolic: 95, mean: 96 },
  { time: '2002-07-15 11:47:00', systolic: 111, diastolic: 65, mean: 79 },
  { time: '2002-07-15 12:00:00', systolic: 117, diastolic: 66, mean: 82 },
  { time: '2002-07-15 12:12:00', systolic: 109, diastolic: 64, mean: 79 },
  { time: '2002-07-15 12:25:00', systolic: 123, diastolic: 71, mean: 87 },
  { time: '2002-07-15 12:37:00', systolic: 112, diastolic: 65, mean: 81 },
];

const MedicalDashboard = () => {
  const [selectedDate, setSelectedDate] = useState(new Date(2002, 6, 15));
  const [currentMonth, setCurrentMonth] = useState(new Date(2002, 6, 1));

  const chartData = useMemo(() => {
    return rawData.map(d => ({
      ...d,
      timeLabel: d.time.split(' ')[1].substring(0, 5)
    }));
  }, []);

  const stats = useMemo(() => {
    const systolicValues = rawData.map(d => d.systolic);
    const diastolicValues = rawData.map(d => d.diastolic);
    return {
      avgSystolic: Math.round(systolicValues.reduce((a, b) => a + b) / systolicValues.length),
      avgDiastolic: Math.round(diastolicValues.reduce((a, b) => a + b) / diastolicValues.length),
      latest: rawData[rawData.length - 1]
    };
  }, []);

  const daysInMonth = new Date(currentMonth.getFullYear(), currentMonth.getMonth() + 1, 0).getDate();
  const firstDayOfMonth = new Date(currentMonth.getFullYear(), currentMonth.getMonth(), 1).getDay();
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  const changeMonth = (delta) => {
    setCurrentMonth(new Date(currentMonth.getFullYear(), currentMonth.getMonth() + delta, 1));
  };

  const isSelectedDate = (day) => {
    return selectedDate.getDate() === day && 
           selectedDate.getMonth() === currentMonth.getMonth() &&
           selectedDate.getFullYear() === currentMonth.getFullYear();
  };

  const exportToCSV = () => {
    const headers = ['datetime,systolic,diastolic,mean'];
    const rows = rawData.map(d => `${d.time},${d.systolic},${d.diastolic},${d.mean}`);
    const csv = [headers, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'blood_pressure_data.csv';
    a.click();
  };

  return (
    <div className="h-screen bg-slate-100 flex">
      {/* Left Sidebar */}
      <div className="w-48 bg-gradient-to-b from-slate-800 to-slate-900 text-white flex flex-col">
        <div className="p-4 border-b border-slate-700">
          <div className="flex items-center gap-2 mb-1">
            <Activity className="w-6 h-6 text-blue-400" />
            <span className="font-bold text-lg">medic+</span>
          </div>
        </div>
        
        <div className="flex-1 p-4">
          <div className="mb-6">
            <div className="w-16 h-16 bg-slate-700 rounded-full mx-auto mb-3 overflow-hidden">
              <div className="w-full h-full bg-gradient-to-br from-blue-400 to-blue-600 flex items-center justify-center">
                <User className="w-8 h-8 text-white" />
              </div>
            </div>
            <div className="text-center">
              <div className="font-semibold text-sm">Linda Moore</div>
              <div className="text-xs text-slate-400">Female, Feb 29, 1968</div>
              <div className="text-xs text-slate-500 mt-1">555-752-8533</div>
            </div>
          </div>
          
          <div className="text-xs space-y-2 text-slate-400">
            <div>
              <div className="text-slate-500">Insurance</div>
              <div className="text-white text-sm">U-Healthcare</div>
              <div className="text-slate-500 text-xs">I-44930</div>
            </div>
            <div className="mt-3">
              <div className="text-slate-500">Address</div>
              <div className="text-white text-xs">505 Cherry St, Concord</div>
            </div>
          </div>
        </div>

        <button 
          onClick={exportToCSV}
          className="m-4 px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg text-sm font-medium flex items-center justify-center gap-2 transition-colors"
        >
          <Download className="w-4 h-4" />
          Export CSV
        </button>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-auto">
        <div className="p-6">
          {/* Header */}
          <div className="mb-6">
            <h1 className="text-3xl font-light text-slate-700 mb-1">Patient Health</h1>
            <div className="flex items-center gap-4 text-sm text-slate-500">
              <button className="px-3 py-1 bg-white rounded-lg border-b-2 border-blue-500 font-medium text-slate-700">Overview</button>
              <button className="px-3 py-1 text-slate-400">History</button>
              <button className="px-3 py-1 text-slate-400">Reports</button>
            </div>
          </div>

          <div className="grid grid-cols-12 gap-4">
            {/* Left Column - Body & BP */}
            <div className="col-span-5 space-y-4">
              {/* Body Diagram */}
              <div className="bg-white rounded-lg shadow-sm p-4">
                <div className="flex gap-4">
                  <div className="flex-1">
                    <svg viewBox="0 0 200 400" className="w-full h-64">
                      {/* Front Body */}
                      <ellipse cx="100" cy="40" rx="25" ry="30" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="80" y="65" width="40" height="60" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="70" y="70" width="15" height="50" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="115" y="70" width="15" height="50" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="85" y="125" width="30" height="80" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="75" y="205" width="20" height="90" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="105" y="205" width="20" height="90" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <circle cx="100" cy="85" r="12" fill="#ef4444" opacity="0.7">
                        <animate attributeName="opacity" values="0.5;1;0.5" dur="2s" repeatCount="indefinite"/>
                      </circle>
                      <text x="100" y="90" fontSize="10" fill="white" textAnchor="middle" fontWeight="bold">❤</text>
                    </svg>
                    <div className="text-center mt-2">
                      <div className="text-xs text-amber-600 font-medium">⚠ This doesn't look good</div>
                      <div className="text-xs text-slate-500">Linda Moore, see a doctor</div>
                    </div>
                  </div>
                  <div className="flex-1">
                    <svg viewBox="0 0 200 400" className="w-full h-64">
                      {/* Back Body */}
                      <ellipse cx="100" cy="40" rx="25" ry="30" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="80" y="65" width="40" height="60" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="70" y="70" width="15" height="50" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="115" y="70" width="15" height="50" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="85" y="125" width="30" height="80" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="75" y="205" width="20" height="90" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                      <rect x="105" y="205" width="20" height="90" rx="8" fill="#e2e8f0" stroke="#cbd5e1" strokeWidth="2"/>
                    </svg>
                  </div>
                </div>
                
                <div className="grid grid-cols-2 gap-3 mt-4 text-sm">
                  <div>
                    <div className="text-slate-500 text-xs">Age</div>
                    <div className="font-semibold text-slate-700">57 yrs</div>
                    <div className="text-xs text-slate-400">avg. weight 11/30/2024</div>
                  </div>
                  <div>
                    <div className="text-slate-500 text-xs">Height</div>
                    <div className="font-semibold text-slate-700">152.8 cm</div>
                  </div>
                  <div className="col-span-2">
                    <div className="text-slate-500 text-xs mb-1">Weight: 72.0 kg</div>
                    <div className="flex items-center gap-1 text-xs">
                      <span className="text-slate-400">added 0.0 kg since last month</span>
                    </div>
                  </div>
                </div>
              </div>

              {/* Blood Pressure */}
              <div className="bg-white rounded-lg shadow-sm p-4">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <Activity className="w-5 h-5 text-red-500" />
                    <h3 className="font-semibold text-slate-700">Blood pressure</h3>
                  </div>
                  <div className="text-right">
                    <div className="text-2xl font-bold text-slate-800">{stats.latest.systolic}/{stats.latest.diastolic}</div>
                    <div className="text-xs text-slate-500">yesterday: {stats.avgSystolic}/{stats.avgDiastolic}</div>
                  </div>
                </div>
                <div className="h-40">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                      <XAxis dataKey="timeLabel" tick={{fontSize: 10}} stroke="#94a3b8" />
                      <YAxis domain={[40, 140]} tick={{fontSize: 10}} stroke="#94a3b8" />
                      <Tooltip contentStyle={{fontSize: 12, borderRadius: 8}} />
                      <Line type="monotone" dataKey="systolic" stroke="#ef4444" strokeWidth={2} dot={{r: 2}} />
                      <Line type="monotone" dataKey="diastolic" stroke="#3b82f6" strokeWidth={2} dot={{r: 2}} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>

            {/* Middle Column - Other Vitals */}
            <div className="col-span-4 space-y-4">
              {/* Blood Count */}
              <div className="bg-white rounded-lg shadow-sm p-4 opacity-50">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <Droplet className="w-5 h-5 text-blue-500" />
                    <h3 className="font-semibold text-slate-700">Blood count</h3>
                  </div>
                  <div className="text-2xl font-bold text-slate-400">--</div>
                </div>
                <div className="h-32 bg-slate-50 rounded flex items-center justify-center">
                  <span className="text-xs text-slate-400">No data available</span>
                </div>
              </div>

              {/* Heart Rate */}
              <div className="bg-white rounded-lg shadow-sm p-4 opacity-50">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <Heart className="w-5 h-5 text-pink-500" />
                    <h3 className="font-semibold text-slate-700">Heart rate</h3>
                  </div>
                  <div className="text-2xl font-bold text-slate-400">--</div>
                </div>
                <div className="h-32 bg-slate-50 rounded flex items-center justify-center">
                  <span className="text-xs text-slate-400">No data available</span>
                </div>
              </div>

              {/* Cholesterol */}
              <div className="bg-white rounded-lg shadow-sm p-4 opacity-50">
                <div className="flex items-center justify-between mb-2">
                  <h3 className="font-semibold text-slate-700">Cholesterol level</h3>
                  <div className="text-2xl font-bold text-slate-400">--</div>
                </div>
                <div className="h-32 bg-slate-50 rounded flex items-center justify-center">
                  <span className="text-xs text-slate-400">No data available</span>
                </div>
              </div>
            </div>

            {/* Right Column - Labs & Calendar */}
            <div className="col-span-3 space-y-4">
              {/* Labs */}
              <div className="bg-white rounded-lg shadow-sm p-4">
                <h3 className="font-semibold text-slate-700 mb-3 text-sm">Labs</h3>
                <div className="text-xs text-slate-500 mb-3">from last appointment</div>
                <div className="space-y-2 opacity-50">
                  {[
                    {name: 'Ca', value: '--', range: '8.5-10.5'},
                    {name: 'K', value: '--', range: '3.5-5.0'},
                    {name: 'Mg', value: '--', range: '1.7-2.2'},
                    {name: 'Na', value: '--', range: '135-145'},
                    {name: 'P', value: '--', range: '2.5-4.5'},
                    {name: 'Cl', value: '--', range: '96-106'},
                  ].map(lab => (
                    <div key={lab.name} className="flex items-center justify-between text-xs">
                      <span className="text-slate-600 w-8">{lab.name}</span>
                      <div className="flex-1 mx-2 h-1.5 bg-slate-200 rounded-full" />
                      <span className="text-slate-400 w-12 text-right">{lab.value}</span>
                    </div>
                  ))}
                </div>
              </div>

              {/* Calendar */}
              <div className="bg-white rounded-lg shadow-sm p-4">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="font-semibold text-slate-700 text-sm">
                    {monthNames[currentMonth.getMonth()]} {currentMonth.getFullYear()}
                  </h3>
                  <div className="flex gap-1">
                    <button onClick={() => changeMonth(-1)} className="p-1 hover:bg-slate-100 rounded">
                      <ChevronLeft className="w-4 h-4" />
                    </button>
                    <button onClick={() => changeMonth(1)} className="p-1 hover:bg-slate-100 rounded">
                      <ChevronRight className="w-4 h-4" />
                    </button>
                  </div>
                </div>
                
                <div className="grid grid-cols-7 gap-1 mb-1">
                  {['S', 'M', 'T', 'W', 'T', 'F', 'S'].map((day, i) => (
                    <div key={i} className="text-center text-xs text-slate-400 py-1">{day}</div>
                  ))}
                </div>
                
                <div className="grid grid-cols-7 gap-1">
                  {[...Array(firstDayOfMonth)].map((_, i) => (
                    <div key={`e-${i}`} />
                  ))}
                  {[...Array(daysInMonth)].map((_, i) => {
                    const day = i + 1;
                    const selected = isSelectedDate(day);
                    const hasData = day === 15 && currentMonth.getMonth() === 6;
                    return (
                      <button
                        key={day}
                        onClick={() => setSelectedDate(new Date(currentMonth.getFullYear(), currentMonth.getMonth(), day))}
                        className={`aspect-square text-xs rounded transition-all ${
                          selected ? 'bg-blue-600 text-white font-semibold' : 
                          hasData ? 'bg-green-100 text-green-700 font-medium' :
                          'hover:bg-slate-100 text-slate-600'
                        }`}
                      >
                        {day}
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default MedicalDashboard;