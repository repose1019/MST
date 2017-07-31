# !/usr/bin/env 
# -*-coding:utf-8-*- 
# @author: 'Repose' 
# @date: 2017/7/31

from PIL import Image
import face_recognition
# image = face_recognition.load_image_file("predict.jpg")
# face_locations = face_recognition.face_locations(image)

# image = face_recognition.load_image_file("predict.jpg")
#
# # Find all the faces in the image
# face_locations = face_recognition.face_locations(image)
#
# print("I found {} face(s) in this photograph.".format(len(face_locations)))
#
# for face_location in face_locations:
#
#     # Print the location of each face in this image
#     top, right, bottom, left = face_location
#     print("A face is located at pixel location Top: {}, Left: {}, Bottom: {}, Right: {}".format(top, left, bottom, right))
#
#     # You can access the actual face itself like this:
#     face_image = image[top:bottom, left:right]
#     pil_image = Image.fromarray(face_image)
#     pil_image.show()


known_image = face_recognition.load_image_file("zhangxin.jpg")
unknown_image = face_recognition.load_image_file("predict.jpg")

biden_encoding = face_recognition.face_encodings(known_image)[0]
for i in range(5):
    unknown_encoding = face_recognition.face_encodings(unknown_image)[i]

    results = face_recognition.compare_faces([biden_encoding], unknown_encoding)
    print(results)