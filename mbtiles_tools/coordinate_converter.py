#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
坐标系统转换模块

支持XYZ和TMS坐标系统之间的转换
"""


class CoordinateConverter:
    """
    坐标系统转换类
    """
    
    @staticmethod
    def tms_to_xyz(zoom, y_tms):
        """
        将TMS坐标转换为XYZ坐标
        
        Args:
            zoom: 缩放级别
            y_tms: TMS格式的y坐标
            
        Returns:
            int: XYZ格式的y坐标
        """
        return (2 ** zoom) - 1 - y_tms
    
    @staticmethod
    def xyz_to_tms(zoom, y_xyz):
        """
        将XYZ坐标转换为TMS坐标
        
        Args:
            zoom: 缩放级别
            y_xyz: XYZ格式的y坐标
            
        Returns:
            int: TMS格式的y坐标
        """
        return (2 ** zoom) - 1 - y_xyz
    
    @staticmethod
    def convert(zoom, y, from_scheme, to_scheme):
        """
        在不同坐标系统之间转换y坐标
        
        Args:
            zoom: 缩放级别
            y: 原始y坐标
            from_scheme: 原始坐标系统 ('xyz' 或 'tms')
            to_scheme: 目标坐标系统 ('xyz' 或 'tms')
            
        Returns:
            int: 转换后的y坐标
        """
        if from_scheme == to_scheme:
            return y
        
        if to_scheme == 'xyz':
            return CoordinateConverter.tms_to_xyz(zoom, y)
        else:
            return CoordinateConverter.xyz_to_tms(zoom, y)
