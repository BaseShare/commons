package com.ctfo.util;

import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;

import javax.imageio.ImageIO;

import com.sun.image.codec.jpeg.JPEGCodec;
import com.sun.image.codec.jpeg.JPEGImageEncoder;

@SuppressWarnings("restriction")
public class ImageUtil {
	/**
	 * 生成缩略图 <br/>
	 * 保存:ImageIO.write(BufferedImage, imgType[jpg/png/...], File);
	 * 
	 * @param source
	 *            原图片
	 * @param width
	 *            缩略图宽
	 * @param height
	 *            缩略图高
	 * @param b
	 *            是否等比缩放
	 * @throws Exception 
	 * */
	public static File Thumb(File file, int width, int height, boolean proportion) throws Exception {
		try {
			// 获得源文件
			if (!file.exists()) {
				throw new Exception("文件不存在！");
			}

			Image img = ImageIO.read(file); // 如果是本地图片处理的话，这个地方直接把file放到ImageIO.read(file)中即可，如果是执行上传图片的话，
											// Formfile formfile=获得表单提交的Formfile
											// ,然后 ImageIO.read 方法里参数放
											// formfile.getInputStream()
			// 判断图片格式是否正确
			if (img.getWidth(null) == -1) {
				throw new Exception("文件格式不正确！");
			} else {
				int newWidth;
				int newHeight;
				// 判断是否是等比缩放
				if (proportion == true) {
					// 为等比缩放计算输出的图片宽度及高度
					double rate1 = ((double) img.getWidth(null))
							/ (double) width + 0.1;
					double rate2 = ((double) img.getHeight(null))
							/ (double) height + 0.1;
					// 根据缩放比率大的进行缩放控制
					double rate = rate1 > rate2 ? rate1 : rate2;
					newWidth = (int) (((double) img.getWidth(null)) / rate);
					newHeight = (int) (((double) img.getHeight(null)) / rate);
				} else {
					newWidth = width; // 输出的图片宽度
					newHeight = height; // 输出的图片高度
				}
				BufferedImage tag = new BufferedImage((int) newWidth,
						(int) newHeight, BufferedImage.TYPE_INT_RGB);

				tag.getGraphics().drawImage(
						img.getScaledInstance(newWidth, newHeight,
								Image.SCALE_SMOOTH), 0, 0, null);
				FileOutputStream out = new FileOutputStream(file.getName());
				// JPEGImageEncoder可适用于其他图片类型的转换
				JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(out);
				encoder.encode(tag);
				out.close();
				
				return new File(file.getName());
			}
		} catch (IOException ex) {
			ex.printStackTrace();
			return null;
		}
	}
	
	public static int getImageWidth(File file) throws Exception{
		if (!file.exists()) {
			throw new Exception("文件不存在！");
		}

		Image img = ImageIO.read(file);
		return img.getWidth(null);
	}
	
	public static int getImageHeight(File file) throws Exception{
		if (!file.exists()) {
			throw new Exception("文件不存在！");
		}

		Image img = ImageIO.read(file);
		return img.getHeight(null);
	}

	/**
	 * 图片水印
	 * 
	 * @param imgPath
	 *            待处理图片
	 * @param markPath
	 *            水印图片
	 * @param x
	 *            水印位于图片左上角的 x 坐标值
	 * @param y
	 *            水印位于图片左上角的 y 坐标值
	 * @param alpha
	 *            水印透明度 0.1f ~ 1.0f
	 * */
	public static File waterMark(File file, float alpha) {
		try {
			URL url = ImageUtil.class.getClassLoader().getResource("water-mark.png");
			// 加载待处理图片文件
			Image img = ImageIO.read(file);
			BufferedImage image = new BufferedImage(img.getWidth(null),
					img.getHeight(null), BufferedImage.TYPE_INT_RGB);
			Graphics2D g = image.createGraphics();
			g.drawImage(img, 0, 0, null);
			// 加载水印图片文件
			Image src_biao = ImageIO.read(url);
			g.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_ATOP,
					alpha));
			g.drawImage(src_biao, (img.getWidth(null)-src_biao.getWidth(null)), (img.getHeight(null)-src_biao.getHeight(null)), null);
			g.dispose();
			// 保存处理后的文件
			FileOutputStream out = new FileOutputStream(file.getName());
			JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(out);
			encoder.encode(image);
			out.close();
			
			return new File(file.getName());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 
	 * 文字水印
	 * @param imgPath
	 *            待处理图片
	 * @param text
	 *            水印文字
	 * @param font
	 *            水印字体信息
	 * @param color
	 *            水印字体颜色
	 * @param x
	 *            水印位于图片左上角的 x 坐标值
	 * @param y
	 *            水印位于图片左上角的 y 坐标值
	 * @param alpha
	 *            水印透明度 0.1f ~ 1.0f
	 */

	public static void textMark(String imgPath, String text, Font font,
			Color color, int x, int y, float alpha) {
		try {
			Font Dfont = (font == null) ? new Font("宋体", 20, 13) : font;
			Image img = ImageIO.read(new File(imgPath));
			BufferedImage image = new BufferedImage(img.getWidth(null),
					img.getHeight(null), BufferedImage.TYPE_INT_RGB);
			Graphics2D g = image.createGraphics();
			g.drawImage(img, 0, 0, null);
			g.setColor(color);
			g.setFont(Dfont);
			g.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_ATOP,
					alpha));
			g.drawString(text, x, y);
			g.dispose();
			FileOutputStream out = new FileOutputStream(imgPath);
			JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(out);
			encoder.encode(image);
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static boolean validateImage(File file) throws Exception{
		Image img;
		try {
			img = ImageIO.read(file);// 判断图片格式是否正确
			if (img.getWidth(null) == -1) {
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}
