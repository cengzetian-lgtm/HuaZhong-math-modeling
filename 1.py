"""
华中杯数学建模 - 城市绿色物流配送调度
数据处理模块
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pickle
import os
import warnings
warnings.filterwarnings('ignore')

# ============================================
# 1. 路径配置
# ============================================
BASE_PATH = r"C:\Users\22059\Desktop\杂物\A题：城市绿色物流配送调度"
DATA_FOLDER = os.path.join(BASE_PATH, "附件")

# 路径确认
print("="*60)
print("路径配置:")
print(f"基础路径: {BASE_PATH}")
print(f"数据文件夹: {DATA_FOLDER}")
print(f"数据文件夹是否存在: {os.path.exists(DATA_FOLDER)}")

if os.path.exists(DATA_FOLDER):
    print("文件夹内容:")
    for file in os.listdir(DATA_FOLDER):
        print(f"  - {file}")
else:
    print("错误: 数据文件夹不存在!")
    exit(1)
print("="*60)

# ============================================
# 2. 设置中文字体
# ============================================
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# ============================================
# 3. 数据加载类
# ============================================
class DataLoader:
    """数据加载器 - 处理所有数据加载和预处理"""
    
    def __init__(self, data_folder=DATA_FOLDER):
        self.data_folder = data_folder
        self.order_data = None
        self.distance_matrix = None
        self.coords = None
        self.time_windows = None
        self.customer_demand = None
        self.green_zone_customers = []
        self.center_x, self.center_y = 0, 0
        self.green_zone_radius = 10
        
        # 车辆信息
        self.vehicles = {
            '燃油车3000kg': {
                'type': '燃油',
                'load_capacity': 3000,
                'volume_capacity': 9,
                'count': 2,
                'start_cost': 400,
                'oil_price': 8.5,
                'co2_factor': 2.6
            },
            '燃油车2500kg': {
                'type': '燃油',
                'load_capacity': 2500,
                'volume_capacity': 8,
                'count': 6,
                'start_cost': 400,
                'oil_price': 8.5,
                'co2_factor': 2.6
            },
            '燃油车2000kg': {
                'type': '燃油',
                'load_capacity': 2000,
                'volume_capacity': 7,
                'count': 6,
                'start_cost': 400,
                'oil_price': 8.5,
                'co2_factor': 2.6
            },
            '新能源车3000kg': {
                'type': '新能源',
                'load_capacity': 3000,
                'volume_capacity': 9,
                'count': 4,
                'start_cost': 400,
                'elec_price': 1.5,
                'co2_factor': 0.7
            },
            '新能源车2000kg': {
                'type': '新能源',
                'load_capacity': 2000,
                'volume_capacity': 7,
                'count': 2,
                'start_cost': 400,
                'elec_price': 1.5,
                'co2_factor': 0.7
            }
        }
        
        # 时段速度信息
        self.period_speeds = {
            "顺畅": {"start": 0, "end": 7, "speed_mean": 50, "speed_std": 5},
            "一般": {"start": 7, "end": 17, "speed_mean": 40, "speed_std": 8},
            "拥堵": {"start": 17, "end": 24, "speed_mean": 30, "speed_std": 10}
        }
    
    def load_all_data(self):
        """加载所有Excel数据文件"""
        print("\n" + "="*60)
        print("开始加载数据...")
        print("="*60)
    
        try:
            # 1. 加载订单信息
            order_path = os.path.join(self.data_folder, "订单信息.xlsx")
            self.order_data = pd.read_excel(order_path)
            print(f"✓ 订单信息加载成功: {self.order_data.shape}")
            
            # 统一列名
            column_mapping = {
                '订单编号': '订单号',
                '目标客户编号': '客户点编号'
            }
            for old, new in column_mapping.items():
                if old in self.order_data.columns:
                    self.order_data = self.order_data.rename(columns={old: new})
            print("✓ 订单信息列名已统一")
        
            # 2. 加载距离矩阵
            dist_path = os.path.join(self.data_folder, "距离矩阵.xlsx")
            self.distance_matrix = pd.read_excel(dist_path, header=None)
            print(f"✓ 距离矩阵加载成功: {self.distance_matrix.shape}")
            
            # 确保距离矩阵为数值类型
            self.distance_matrix = self.distance_matrix.apply(pd.to_numeric, errors='coerce')
            nan_count = self.distance_matrix.isna().sum().sum()
            if nan_count > 0:
                self.distance_matrix = self.distance_matrix.fillna(0)
            
            self.distance_matrix = self.distance_matrix.astype(float)
            print("✓ 距离矩阵数据类型已修复")
        
            # 3. 加载客户坐标
            coord_path = os.path.join(self.data_folder, "客户坐标信息.xlsx")
            self.coords = pd.read_excel(coord_path)
            print(f"✓ 客户坐标加载成功: {self.coords.shape}")
            
            # 统一坐标列名
            coord_mapping = {
                'ID': '客户点编号',
                'X (km)': 'X坐标',
                'Y (km)': 'Y坐标'
            }
            for old, new in coord_mapping.items():
                if old in self.coords.columns:
                    self.coords = self.coords.rename(columns={old: new})
            print("✓ 客户坐标列名已统一")
        
            # 4. 加载时间窗
            time_path = os.path.join(self.data_folder, "时间窗.xlsx")
            self.time_windows = pd.read_excel(time_path)
            print(f"✓ 时间窗加载成功: {self.time_windows.shape}")
            
            # 统一时间窗列名
            time_mapping = {
                '客户编号': '客户点编号',
                '开始时间': '最早时间',
                '结束时间': '最晚时间'
            }
            for old, new in time_mapping.items():
                if old in self.time_windows.columns:
                    self.time_windows = self.time_windows.rename(columns={old: new})
            print("✓ 时间窗列名已统一")
        
            print("\n✓ 所有数据加载完成！")
            return True
        
        except Exception as e:
            print(f"✗ 加载数据时出错: {e}")
            return False
    
    def preprocess_data(self):
        """数据预处理"""
        print("\n" + "="*60)
        print("开始数据预处理...")
        print("="*60)
        
        # 1. 按客户汇总订单
        if '客户点编号' in self.order_data.columns and '重量' in self.order_data.columns and '体积' in self.order_data.columns:
            self.customer_demand = self.order_data.groupby('客户点编号').agg({
                '重量': 'sum',
                '体积': 'sum',
                '订单号': 'count'
            }).rename(columns={'订单号': '订单数量'})
            print(f"✓ 已按客户汇总订单: {len(self.customer_demand)} 个客户")
        else:
            print("✗ 订单信息列名不匹配")
            return False
        
        # 2. 转换时间窗为小时制
        if '最早时间' in self.time_windows.columns and '最晚时间' in self.time_windows.columns:
            self.time_windows['最早小时'] = self.time_windows['最早时间'].apply(self._time_to_hours)
            self.time_windows['最晚小时'] = self.time_windows['最晚时间'].apply(self._time_to_hours)
            self.time_windows['时间窗长度'] = self.time_windows['最晚小时'] - self.time_windows['最早小时']
            print(f"✓ 时间窗已转换为小时制")
        else:
            print("✗ 时间窗列名不匹配")
            return False
        
        # 3. 识别绿色配送区内的客户
        if 'X坐标' in self.coords.columns and 'Y坐标' in self.coords.columns:
            distances = np.sqrt((self.coords['X坐标'] - self.center_x)**2 + 
                              (self.coords['Y坐标'] - self.center_y)**2)
            self.green_zone_customers = self.coords[distances <= self.green_zone_radius].index.tolist()
            if 0 in self.green_zone_customers:
                self.green_zone_customers.remove(0)
            print(f"✓ 绿色配送区内客户: {len(self.green_zone_customers)} 个")
        else:
            print("✗ 坐标列名不匹配")
            return False
        
        # 4. 检查数据一致性
        print("\n" + "="*60)
        print("数据一致性检查:")
        print("="*60)
        
        n_customers_coords = len(self.coords)
        n_customers_time = len(self.time_windows)
        
        print(f"坐标文件中的点数: {n_customers_coords}")
        print(f"时间窗文件中的客户数: {n_customers_time}")
        
        if self.distance_matrix.shape[0] == self.distance_matrix.shape[1] == n_customers_coords:
            print(f"✓ 距离矩阵维度匹配: {self.distance_matrix.shape}")
        else:
            print(f"✗ 距离矩阵维度不匹配: {self.distance_matrix.shape}")
        
        order_customers = set(self.customer_demand.index)
        coord_customers = set(range(n_customers_coords))
        missing_in_coords = order_customers - coord_customers
        if missing_in_coords:
            print(f"✗ 订单中有{len(missing_in_coords)}个客户在坐标文件中不存在")
        else:
            print("✓ 订单客户全部在坐标文件中")
        
        return True
    
    def _time_to_hours(self, time_val):
        """将时间转换为小时制的小数"""
        if pd.isna(time_val):
            return 0
        
        if isinstance(time_val, str):
            if ':' in time_val:
                parts = time_val.split(':')
                hours = int(parts[0])
                minutes = int(parts[1]) if len(parts) > 1 else 0
                return hours + minutes/60
            else:
                try:
                    return float(time_val)
                except:
                    return 0
        elif isinstance(time_val, (int, float)):
            return float(time_val)
        elif hasattr(time_val, 'hour'):
            return time_val.hour + time_val.minute/60
        else:
            return 0
    
    def get_customer_info(self, customer_id):
        """获取指定客户的完整信息"""
        info = {}
        
        if customer_id < len(self.coords):
            info['坐标'] = (self.coords.loc[customer_id, 'X坐标'], 
                          self.coords.loc[customer_id, 'Y坐标'])
        
        if customer_id < len(self.time_windows):
            info['最早时间'] = self.time_windows.loc[customer_id, '最早小时']
            info['最晚时间'] = self.time_windows.loc[customer_id, '最晚小时']
        
        if customer_id in self.customer_demand.index:
            info['重量'] = self.customer_demand.loc[customer_id, '重量']
            info['体积'] = self.customer_demand.loc[customer_id, '体积']
            info['订单数量'] = self.customer_demand.loc[customer_id, '订单数量']
        
        info['在绿色配送区'] = customer_id in self.green_zone_customers
        
        return info
    
    def calculate_travel_time(self, start_time_h, distance_km, period_speeds=None):
        """
        计算在特定时间出发，行驶一段距离所需的时间
        
        参数:
        start_time_h: 出发时间（小时）
        distance_km: 行驶距离（公里）
        period_speeds: 时段速度字典
        
        返回:
        travel_time_h: 行驶时间（小时）
        arrival_time_h: 到达时间（小时）
        """
        if period_speeds is None:
            period_speeds = self.period_speeds
        
        remaining_distance = distance_km
        current_time = start_time_h % 24
        
        while remaining_distance > 0:
            current_period = None
            for period, info in period_speeds.items():
                if info["start"] <= current_time < info["end"]:
                    current_period = period
                    break
            
            if current_period is None:
                current_time = 0
                continue
            
            speed = period_speeds[current_period]["speed_mean"]
            period_end = period_speeds[current_period]["end"]
            
            time_in_period = period_end - current_time
            
            if time_in_period <= 0:
                current_time = 0
                continue
            
            distance_in_period = speed * time_in_period
            
            if distance_in_period >= remaining_distance:
                travel_time = remaining_distance / speed
                remaining_distance = 0
                current_time += travel_time
            else:
                remaining_distance -= distance_in_period
                current_time = period_end
        
        travel_time_h = current_time - (start_time_h % 24)
        if travel_time_h < 0:
            travel_time_h += 24
        
        arrival_time_h = start_time_h + travel_time_h
        
        return travel_time_h, arrival_time_h
    
    def get_distance(self, from_node, to_node):
        """获取两点之间的距离"""
        if from_node < self.distance_matrix.shape[0] and to_node < self.distance_matrix.shape[1]:
            return self.distance_matrix.iloc[from_node, to_node]
        else:
            print(f"警告: 节点索引超出范围 ({from_node}, {to_node})")
            return 0
    
    def visualize_data(self, save_fig=True):
        """可视化数据"""
        fig = plt.figure(figsize=(16, 10))
        
        # 1. 客户分布图
        ax1 = plt.subplot(2, 3, 1)
        colors = ['green' if i in self.green_zone_customers else 'blue' for i in range(len(self.coords))]
        sizes = [200 if i == 0 else 30 for i in range(len(self.coords))]
        
        ax1.scatter(self.coords['X坐标'], self.coords['Y坐标'], 
                   c=colors[:len(self.coords)], s=sizes[:len(self.coords)], alpha=0.6)
        
        ax1.scatter(self.coords.loc[0, 'X坐标'], self.coords.loc[0, 'Y坐标'], 
                   c='red', s=300, marker='*', label='配送中心')
        
        circle = plt.Circle((self.center_x, self.center_y), self.green_zone_radius, 
                           color='green', fill=False, linewidth=2, linestyle='--', 
                           label='绿色配送区')
        ax1.add_patch(circle)
        
        ax1.set_xlabel('X坐标 (km)')
        ax1.set_ylabel('Y坐标 (km)')
        ax1.set_title('客户点分布')
        ax1.grid(True, alpha=0.3)
        ax1.axis('equal')
        ax1.legend()
        
        # 2. 客户需求量分布
        ax2 = plt.subplot(2, 3, 2)
        if self.customer_demand is not None:
            ax2.hist(self.customer_demand['重量'], bins=20, edgecolor='black', alpha=0.7)
            ax2.set_xlabel('重量 (kg)')
            ax2.set_ylabel('客户数量')
            ax2.set_title('客户重量需求分布')
            ax2.grid(True, alpha=0.3)
        
        # 3. 时间窗分布
        ax3 = plt.subplot(2, 3, 3)
        if self.time_windows is not None and '最早小时' in self.time_windows.columns:
            ax3.hist(self.time_windows['最早小时'], bins=20, alpha=0.5, label='最早时间')
            ax3.hist(self.time_windows['最晚小时'], bins=20, alpha=0.5, label='最晚时间')
            ax3.set_xlabel('时间 (小时)')
            ax3.set_ylabel('客户数量')
            ax3.set_title('时间窗分布')
            ax3.legend()
            ax3.grid(True, alpha=0.3)
        
        # 4. 时间窗长度分布
        ax4 = plt.subplot(2, 3, 4)
        if self.time_windows is not None and '时间窗长度' in self.time_windows.columns:
            ax4.hist(self.time_windows['时间窗长度'], bins=20, edgecolor='black', alpha=0.7, color='orange')
            ax4.set_xlabel('时间窗长度 (小时)')
            ax4.set_ylabel('客户数量')
            ax4.set_title('时间窗长度分布')
            ax4.grid(True, alpha=0.3)
        
        # 5. 客户体积需求分布
        ax5 = plt.subplot(2, 3, 5)
        if self.customer_demand is not None:
            ax5.hist(self.customer_demand['体积'], bins=20, edgecolor='black', alpha=0.7, color='purple')
            ax5.set_xlabel('体积 (m³)')
            ax5.set_ylabel('客户数量')
            ax5.set_title('客户体积需求分布')
            ax5.grid(True, alpha=0.3)
        
        # 6. 距离矩阵热力图
        ax6 = plt.subplot(2, 3, 6)
        if self.distance_matrix is not None:
            size = min(20, self.distance_matrix.shape[0])
            
            try:
                heatmap_data = self.distance_matrix.iloc[:size, :size].values
                
                if heatmap_data.dtype == 'object':
                    heatmap_data = heatmap_data.astype(float)
                
                if np.any(np.isnan(heatmap_data)):
                    heatmap_data = np.nan_to_num(heatmap_data)
                
                im = ax6.imshow(heatmap_data, cmap='hot', aspect='auto')
                ax6.set_title(f'距离矩阵热力图 (前{size}x{size})')
                ax6.set_xlabel('目标节点')
                ax6.set_ylabel('出发节点')
                plt.colorbar(im, ax=ax6, label='距离 (km)')
            except Exception as e:
                ax6.text(0.5, 0.5, '热力图绘制失败', ha='center', va='center', transform=ax6.transAxes)
                ax6.set_title('距离矩阵热力图 (绘制失败)')
        
        plt.suptitle('数据可视化分析', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        if save_fig:
            output_path = os.path.join(BASE_PATH, 'data_visualization.png')
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            print(f"✓ 可视化图表已保存为 '{output_path}'")
        
        plt.show()
    
    def generate_summary_report(self):
        """生成数据摘要报告"""
        print("\n" + "="*60)
        print("数据摘要报告")
        print("="*60)
        
        if self.customer_demand is not None:
            print(f"1. 订单统计:")
            print(f"   - 总订单数: {len(self.order_data):,}")
            print(f"   - 总重量: {self.order_data['重量'].sum():,.2f} kg")
            print(f"   - 总体积: {self.order_data['体积'].sum():,.2f} m³")
            print(f"   - 有订单的客户数: {len(self.customer_demand)}")
        
        print(f"\n2. 客户与坐标:")
        print(f"   - 总点数（含配送中心）: {len(self.coords)}")
        print(f"   - 绿色配送区内客户数: {len(self.green_zone_customers)}")
        
        if self.time_windows is not None and '最早小时' in self.time_windows.columns:
            print(f"\n3. 时间窗统计:")
            print(f"   - 最早服务时间: {self.time_windows['最早小时'].min():.2f}:00")
            print(f"   - 最晚服务时间: {self.time_windows['最晚小时'].max():.2f}:00")
            print(f"   - 平均时间窗长度: {self.time_windows['时间窗长度'].mean():.2f} 小时")
        
        if self.distance_matrix is not None:
            print(f"\n4. 距离矩阵:")
            print(f"   - 维度: {self.distance_matrix.shape}")
            try:
                print(f"   - 平均距离: {self.distance_matrix.values.mean():.2f} km")
                print(f"   - 最大距离: {self.distance_matrix.values.max():.2f} km")
                print(f"   - 最小距离: {self.distance_matrix.values.min():.2f} km")
            except Exception as e:
                print(f"    - 距离统计：计算失败")
        
        print(f"\n5. 车辆资源:")
        total_vehicles = sum(v['count'] for v in self.vehicles.values())
        print(f"   - 总车辆数: {total_vehicles}")
        for name, info in self.vehicles.items():
            print(f"   - {name}: {info['count']}辆, 载重{info['load_capacity']}kg, 容积{info['volume_capacity']}m³")
        
        if self.customer_demand is not None:
            total_weight = self.order_data['重量'].sum()
            total_volume = self.order_data['体积'].sum()
            
            max_load_capacity = sum(v['count'] * v['load_capacity'] for v in self.vehicles.values())
            max_volume_capacity = sum(v['count'] * v['volume_capacity'] for v in self.vehicles.values())
            
            print(f"\n6. 运力需求分析:")
            print(f"   - 总重量需求: {total_weight:,.2f} kg")
            print(f"   - 总体积需求: {total_volume:,.2f} m³")
            print(f"   - 总载重能力: {max_load_capacity:,.2f} kg")
            print(f"   - 总体积能力: {max_volume_capacity:,.2f} m³")
            
            min_trips_by_weight = np.ceil(total_weight / max_load_capacity)
            min_trips_by_volume = np.ceil(total_volume / max_volume_capacity)
        
            print(f"\n7. 理论最少运输趟数:")
            print(f"   - 按重量计算: {int(min_trips_by_weight)} 趟")
            print(f"   - 按体积计算: {int(min_trips_by_volume)} 趟")
            print(f"   - 注意：实际需要更多趟，因为车辆需要返回配送中心，且路径有约束")
    
        print("="*60)
    
    def save_processed_data(self, filename='processed_data.pkl'):
        """保存处理后的数据"""
        data_to_save = {
            'order_data': self.order_data,
            'distance_matrix': self.distance_matrix,
            'coords': self.coords,
            'time_windows': self.time_windows,
            'customer_demand': self.customer_demand,
            'green_zone_customers': self.green_zone_customers,
            'vehicles': self.vehicles,
            'period_speeds': self.period_speeds
        }
        
        output_path = os.path.join(BASE_PATH, filename)
        with open(output_path, 'wb') as f:
            pickle.dump(data_to_save, f)
        
        print(f"✓ 处理后的数据已保存到 '{output_path}'")
    
    def test_calculation_functions(self):
        """测试计算函数"""
        print("\n" + "="*60)
        print("测试计算函数")
        print("="*60)
        
        # 测试距离获取
        print("1. 距离获取测试:")
        test_nodes = [(0, 1), (1, 2), (2, 3)]
        for from_node, to_node in test_nodes:
            dist = self.get_distance(from_node, to_node)
            print(f"   从节点{from_node}到节点{to_node}的距离: {dist:.2f} km")
        
        # 测试行驶时间计算
        print("\n2. 行驶时间计算测试:")
        test_cases = [
            (8.5, 50),
            (16.5, 80),
            (20.0, 120)
        ]
        
        for start_time, distance in test_cases:
            travel_time, arrival_time = self.calculate_travel_time(start_time, distance)
            start_str = f"{int(start_time)}:{int((start_time%1)*60):02d}"
            arrival_str = f"{int(arrival_time)}:{int((arrival_time%1)*60):02d}"
            print(f"   {start_str}出发，行驶{distance}km: "
                  f"需要{travel_time:.2f}小时，到达时间{arrival_str}")
        
        # 测试客户信息获取
        print("\n3. 客户信息获取测试:")
        test_customers = [0, 1, 10]
        for cust_id in test_customers:
            info = self.get_customer_info(cust_id)
            print(f"   客户{cust_id}:")
            if '坐标' in info:
                print(f"     坐标: {info['坐标']}")
            if '重量' in info:
                print(f"     重量: {info['重量']:.2f}kg, 体积: {info['体积']:.2f}m³")
            if '最早时间' in info:
                print(f"     时间窗: {info['最早时间']:.2f} - {info['最晚时间']:.2f}")
            print(f"     在绿色配送区: {info.get('在绿色配送区', False)}")


# ============================================
# 4. 主函数
# ============================================
def main():
    """主函数"""
    print("华中杯数学建模 - 城市绿色物流配送调度数据处理")
    print("="*60)
    
    # 创建数据加载器
    loader = DataLoader()
    
    # 加载数据
    if loader.load_all_data():
        # 预处理数据
        if loader.preprocess_data():
            # 生成报告
            loader.generate_summary_report()
            
            # 可视化数据
            loader.visualize_data(save_fig=True)
            
            # 测试计算函数
            loader.test_calculation_functions()
            
            # 保存处理后的数据
            loader.save_processed_data('processed_data.pkl')
            
            print("\n" + "="*60)
            print("数据处理完成！")
            print("="*60)
            print("\n下一步建议:")
            print("1. 检查上面的报告，确保数据质量")
            print("2. 开始设计车辆路径优化算法")
            print("3. 在优化算法中调用loader中的函数:")
            print("   - get_distance(from_node, to_node) 获取距离")
            print("   - calculate_travel_time(start_time, distance) 计算行驶时间")
            print("   - get_customer_info(customer_id) 获取客户信息")
        else:
            print("✗ 数据预处理失败")
    else:
        print("✗ 数据加载失败")
    
    print("\n祝你建模顺利！")


# ============================================
# 5. 运行主函数
# ============================================
if __name__ == "__main__":
    main()